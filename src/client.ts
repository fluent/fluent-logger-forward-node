import {
  ConfigError,
  DataTypeError,
  MissingTagError,
  AckTimeoutError,
  ShutdownError,
} from "./error";
import EventTime from "./event_time";
import * as protocol from "./protocol";
import {FluentAuthOptions, FluentAuthSocket} from "./auth";
import {FluentSocketOptions, FluentSocket} from "./socket";
import {
  Queue,
  PackedForwardQueue,
  CompressedPackedForwardQueue,
  MessageQueue,
  ForwardQueue,
} from "./modes";
import {DeferredPromise} from "p-defer";
import * as crypto from "crypto";
import {EventRetrier, EventRetryOptions} from "./event_retrier";

type EventModes =
  | "Message"
  | "Forward"
  | "PackedForward"
  | "CompressedPackedForward";

type Timestamp = number | Date | EventTime;

type AckOptions = {
  ackTimeout: number;
};

type AckData = {
  timeoutId: NodeJS.Timeout;
  deferred: DeferredPromise<void>;
};

export type FluentClientOptions = {
  eventMode?: EventModes;
  socket?: FluentSocketOptions;
  security?: FluentAuthOptions;
  ack?: Partial<AckOptions>;
  flushInterval?: number;
  milliseconds?: boolean;
  sendQueueFlushSize?: number;
  sendQueueFlushLength?: number;
  sendQueueMaxSize?: number;
  sendQueueMaxLength?: number;
  onSocketError?: (err: Error) => void;
  eventRetry?: Partial<EventRetryOptions>;
};

export class FluentClient {
  private tag_prefix: string;
  private eventMode: EventModes;
  private ackEnabled: boolean;
  private ackOptions: AckOptions;
  private ackQueue: Map<protocol.Chunk, AckData> = new Map();
  private sendQueue: Queue;
  private timeResolution: number;
  private retrier: EventRetrier | null;

  private socket: FluentSocket;

  private flushInterval: number;
  private sendQueueFlushSize: number;
  private sendQueueFlushLength: number;

  private sendQueueMaxSize: number;
  private sendQueueMaxLength: number;

  private nextFlushTimeoutId: null | NodeJS.Timeout = null;
  private flushing = false;
  private willFlushNextTick: Promise<boolean> | null = null;

  constructor(tag_prefix: string, options: FluentClientOptions = {}) {
    options = options || {};
    this.eventMode = options.eventMode || "PackedForward";
    if (this.eventMode === "Message") {
      this.sendQueue = new MessageQueue();
    } else if (this.eventMode === "Forward") {
      this.sendQueue = new ForwardQueue();
    } else if (this.eventMode === "PackedForward") {
      this.sendQueue = new PackedForwardQueue();
    } else if (this.eventMode === "CompressedPackedForward") {
      this.sendQueue = new CompressedPackedForwardQueue();
    } else {
      throw new ConfigError("Unknown event mode: " + this.eventMode);
    }
    if (options.eventRetry) {
      this.retrier = new EventRetrier(options.eventRetry);
    } else {
      this.retrier = null;
    }
    this.tag_prefix = tag_prefix;
    this.ackEnabled = !!options.ack;
    this.ackOptions = {
      ackTimeout: 500,
      ...(options.ack || {}),
    };
    this.timeResolution = options.milliseconds ? 1 : 1000;

    this.flushInterval = options.flushInterval || 0;
    this.sendQueueFlushSize = options.sendQueueFlushSize || +Infinity;
    this.sendQueueFlushLength = options.sendQueueFlushLength || +Infinity;
    this.sendQueueMaxSize =
      options.sendQueueMaxSize || 2 * this.sendQueueFlushSize;
    this.sendQueueMaxLength =
      options.sendQueueMaxLength || 2 * this.sendQueueFlushSize;

    this.socket = this.createSocket(options.security, options.socket);

    this.socket.on("writable", () => this.handleWritable());
    this.socket.on("ack", (chunkId: string) => this.handleAck(chunkId));
    if (options.onSocketError) {
      this.socket.on("error", options.onSocketError);
    }

    this.connect();
  }

  private createSocket(
    security?: FluentAuthOptions,
    options?: FluentSocketOptions
  ): FluentSocket {
    if (security) {
      return new FluentAuthSocket(security, options);
    } else {
      return new FluentSocket(options);
    }
  }

  emit(data: protocol.EventRecord): Promise<void>;
  emit(data: protocol.EventRecord, timestamp: Timestamp): Promise<void>;
  emit(label: string, data: protocol.EventRecord): Promise<void>;
  emit(
    label: string,
    data: protocol.EventRecord,
    timestamp: Timestamp
  ): Promise<void>;
  emit(
    a: string | protocol.EventRecord,
    b?: protocol.EventRecord | Timestamp,
    c?: Timestamp
  ): Promise<void> {
    let label: string | null,
      data: protocol.EventRecord,
      timestamp: Timestamp | null;
    if (typeof a === "string") {
      label = a;
      if (typeof b === "object") {
        data = b;
      } else {
        return Promise.reject(new DataTypeError("data must be an object"));
      }
      if (
        !c ||
        typeof c === "number" ||
        c instanceof Date ||
        c instanceof EventTime
      ) {
        timestamp = c || null;
      } else {
        return Promise.reject(
          new DataTypeError("timestamp was not a valid timestamp")
        );
      }
    } else {
      label = null;
      if (typeof a === "object") {
        data = a;
      } else {
        return Promise.reject(new DataTypeError("data must be an object"));
      }
      if (
        !b ||
        typeof b === "number" ||
        b instanceof Date ||
        b instanceof EventTime
      ) {
        timestamp = b || null;
      } else {
        return Promise.reject(
          new DataTypeError("timestamp was not a valid timestamp")
        );
      }
    }

    const tag = this.makeTag(label);
    if (tag === null || tag.length === 0) {
      return Promise.reject(new MissingTagError("tag is missing"));
    }
    let time: protocol.Time;
    if (
      timestamp === null ||
      (typeof timestamp !== "number" && !(timestamp instanceof EventTime))
    ) {
      time = Math.floor(
        (timestamp ? timestamp.getTime() : Date.now()) / this.timeResolution
      );
    } else {
      time = timestamp;
    }
    if (this.retrier !== null) {
      return this.retrier.retryPromise(() => this.sendEvent(tag, time, data));
    } else {
      return this.sendEvent(tag, time, data);
    }
  }

  private sendEvent(
    tag: protocol.Tag,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void> {
    const promise = this.sendQueue.push(tag, time, data);
    if (this.sendQueue.queueSize > this.sendQueueMaxSize) {
      while (this.sendQueue.queueSize > this.sendQueueMaxSize) {
        this.sendQueue.dropEntry();
      }
    }
    if (this.sendQueue.queueLength > this.sendQueueMaxLength) {
      while (this.sendQueue.queueLength > this.sendQueueMaxLength) {
        this.sendQueue.dropEntry();
      }
    }
    this.maybeFlush();
    return promise;
  }

  private handleWritable() {
    this.maybeFlush();
  }

  public connect() {
    this.socket.connect();
  }

  /**
   * Closes the socket, and clears both the ackQueue and the sendQueue, rejecting all pending events.
   *
   * For use during shutdown events, where we don't plan on reconnecting
   */
  public async shutdown(): Promise<void> {
    try {
      await this.disconnect();
    } finally {
      this.sendQueue.clear();
    }
  }

  /**
   * Closes the socket and clears the ackQueue. Keeps pending events, which can be sent via a later .connect()
   */
  public async disconnect(): Promise<void> {
    try {
      await this.flush();
    } finally {
      try {
        await this.socket.disconnect();
      } finally {
        this.clearAcks();
      }
    }
  }

  private makeTag(label: string | null): string | null {
    let tag = null;
    if (this.tag_prefix && label) {
      tag = `${this.tag_prefix}.${label}`;
    } else if (this.tag_prefix) {
      tag = this.tag_prefix;
    } else if (label) {
      tag = label;
    }
    return tag;
  }

  private innerFlush(): boolean {
    if (this.sendQueue.queueLength === 0) {
      return false;
    }
    if (this.flushing) {
      return this.sendQueue.queueLength > 0;
    }
    if (!this.socket.writable()) {
      return this.sendQueue.queueLength > 0;
    }
    this.flushing = true;
    if (this.nextFlushTimeoutId !== null) {
      clearTimeout(this.nextFlushTimeoutId);
      this.nextFlushTimeoutId = null;
    }

    let availableEvents = true;
    while (availableEvents && this.socket.writable()) {
      availableEvents = this.sendNext();
    }

    this.flushing = false;

    return availableEvents;
  }

  public flush(): Promise<boolean> {
    // Prevent duplicate flushes next tick
    if (this.willFlushNextTick === null) {
      this.willFlushNextTick = new Promise(resolve =>
        process.nextTick(() => {
          this.willFlushNextTick = null;
          resolve(this.innerFlush());
        })
      );
    }
    return this.willFlushNextTick;
  }

  private maybeFlush(): void {
    // nothing to flush
    if (this.sendQueue.queueLength === 0) {
      return;
    }
    // can't flush
    if (!this.socket.writable()) {
      return;
    }
    // flush on an interval
    if (this.flushInterval > 0) {
      if (
        this.sendQueue.queueSize !== -1 &&
        this.sendQueue.queueSize >= this.sendQueueFlushSize
      ) {
        // If the queue has hit the memory flush limit
        this.flush();
      } else if (
        this.sendQueue.queueLength !== -1 &&
        this.sendQueue.queueLength >= this.sendQueueFlushLength
      ) {
        // If the queue has hit the length flush limit
        this.flush();
      } else if (this.nextFlushTimeoutId === null) {
        // Otherwise, schedule the next flush interval
        this.nextFlushTimeoutId = setTimeout(() => {
          this.flush();
        }, this.flushInterval);
      }
    } else {
      // If we're not flushing on an interval, then try to flush on every emission
      this.flush();
    }
  }

  private sendNext(): boolean {
    let chunk: protocol.Chunk | undefined;
    if (this.ackEnabled) {
      chunk = crypto.randomBytes(16).toString("base64");
    }
    const nextPacket = this.sendQueue.nextPacket(chunk);
    if (nextPacket === null) {
      return false;
    }
    // Set up the ack before the write, in case of an immediate response
    if (this.ackEnabled && chunk) {
      this.ackQueue.set(chunk, {
        timeoutId: this.setupAckTimeout(
          chunk,
          nextPacket.deferred,
          this.ackOptions.ackTimeout
        ),
        deferred: nextPacket.deferred,
      });
    }
    // Not awaiting because we don't need to wait for this chunk to be flushed to the kernel buffer
    const writePromise = this.socket.write(nextPacket.packet);

    // However, we do still want to catch errors
    writePromise.catch(err => {
      // If the chunk was put in the ack queue, and is still there, the deferred hasn't been resolved
      if (chunk && this.ackQueue.has(chunk)) {
        const ackTimeoutId = this.ackQueue.get(chunk)?.timeoutId;
        this.ackQueue.delete(chunk);
        if (ackTimeoutId) {
          clearTimeout(ackTimeoutId);
        }
      }
      nextPacket.deferred.reject(err);
    });

    if (!chunk) {
      // Wait for the promise to resolve before resolving the deferred
      writePromise.then(() => nextPacket.deferred.resolve());
    }
    return true;
  }

  private setupAckTimeout(
    chunkId: string,
    deferred: DeferredPromise<void>,
    ackTimeout: number
  ): NodeJS.Timeout {
    return setTimeout(() => {
      if (this.ackQueue.has(chunkId)) {
        this.ackQueue.delete(chunkId);
      }
      deferred.reject(new AckTimeoutError("ack response timeout"));
    }, ackTimeout);
  }

  private handleAck(chunkId: string): void {
    if (!this.ackQueue.has(chunkId)) {
      // Timed out or socket shut down fully before this event could be processed
      return;
    }
    const ackData = this.ackQueue.get(chunkId);
    this.ackQueue.delete(chunkId);
    if (ackData) {
      clearTimeout(ackData.timeoutId);
      ackData.deferred.resolve();
    }
  }

  private clearAcks(): void {
    for (const data of this.ackQueue.values()) {
      clearTimeout(data.timeoutId);
      data.deferred.reject(new ShutdownError("ack queue emptied"));
    }
    this.ackQueue = new Map();
  }
}
