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

type AckData = {
  timeoutId: NodeJS.Timeout;
  deferred: DeferredPromise<void>;
};

/**
 * The set of accepted event modes. See [Forward protocol spec](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#event-modes)
 *
 * `Message` will send each event to FluentD individually.
 *
 * `Forward` will collect the events together by tag, and send them together to FluentD in a single packet.
 * This is more efficient with a `flushInterval` to batch these together.
 *
 * `PackedForward` will behave the same as `Forward`, but will pack the events as part of entering the queue. This saves memory and bandwidth.
 *
 * `CompressedPackedForward` will behave the same as `PackedForward`, but additionally compress the items before emission, saving more bandwidth.
 */
export type EventModes =
  | "Message"
  | "Forward"
  | "PackedForward"
  | "CompressedPackedForward";

/**
 * The set of accepted Timestamp values
 */
export type Timestamp = number | Date | EventTime;

/**
 * Acknowledgement settings
 */
export type AckOptions = {
  /**
   * How long to wait for an acknowledgement from the server
   */
  ackTimeout: number;
};

export type SendQueueLimit = {
  /**
   * The queue size limit (memory)
   *
   * This checks the memory size of the queue, which is only useful with `PackedForward` and `PackedForwardCompressed`.
   *
   * Defaults to +Infinity
   */
  size: number;
  /**
   * The queue length limit (# of entries)
   *
   * This checks the number of events in the queue, which is useful with all event modes.
   *
   * Defaults to +Infinity
   */
  length: number;
};

/**
 * The constructor options passed to the client
 */
export type FluentClientOptions = {
  /**
   * The event mode to use. Defaults to PackedForward
   */
  eventMode?: EventModes;
  /**
   * The connection options. See subtype for defaults.
   */
  socket?: FluentSocketOptions;
  /**
   * The fluentd security options. See subtype for defaults.
   */
  security?: FluentAuthOptions;
  /**
   * Acknowledgement settings.
   */
  ack?: Partial<AckOptions>;
  /**
   * How long to wait to flush the queued events
   *
   * Defaults to 0
   */
  flushInterval?: number;
  /**
   * The timestamp resolution of events passed to FluentD.
   *
   * Defaults to false (seconds). If true, the resolution will be in milliseconds
   */
  milliseconds?: boolean;
  /**
   * The limit at which the queue needs to be flushed.
   *
   * Useful with flushInterval to limit the size of the queue
   *
   * See the subtype for defaults
   */
  sendQueueFlushLimit?: Partial<SendQueueLimit>;

  /**
   * The limit at which we start dropping events
   *
   * Prevents the queue from growing to an unbounded size and exhausting memory.
   *
   * See the subtype for defaults
   */
  sendQueueMaxLimit?: Partial<SendQueueLimit>;
  /**
   * The limit at which we start dropping events when we're not writable
   *
   * Prevents the queue from growing too much when fluentd is down for an extended period
   *
   * Defaults to null (no limit)
   *
   * See the subtype for defaults
   */
  sendQueueNotFlushableLimit?: Partial<SendQueueLimit>;
  /**
   * An error handler which will receive socket error events
   *
   * Useful for logging, these will be handled internally
   */
  onSocketError?: (err: Error) => void;
  /**
   * Retry event submission on failure
   *
   * Warning: This effectively keeps the event in memory until it is successfully sent or retries exhausted
   *
   * See subtype for defaults
   */
  eventRetry?: Partial<EventRetryOptions>;
};

/**
 * A Fluent Client. Connects to a FluentD server using the [Forward protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1).
 */
export class FluentClient {
  private tag_prefix: string | null;
  private eventMode: EventModes;
  private ackEnabled: boolean;
  private ackOptions: AckOptions;
  private ackQueue: Map<protocol.Chunk, AckData> = new Map();
  private sendQueue: Queue;
  private timeResolution: number;
  private retrier: EventRetrier | null;

  private socket: FluentSocket;

  private flushInterval: number;
  private sendQueueFlushLimit: SendQueueLimit;
  private sendQueueMaxLimit: SendQueueLimit;
  private sendQueueNotFlushableLimit: SendQueueLimit | null;

  private nextFlushTimeoutId: null | NodeJS.Timeout = null;
  private flushing = false;
  private willFlushNextTick: Promise<boolean> | null = null;

  /**
   * Creates a new FluentClient
   *
   * @param tag_prefix A prefix to prefix to all tags. For example, passing the prefix "foo" will cause `emit("bar", data)` to emit with `foo.bar`.
   * @param options The client options
   */
  constructor(
    tag_prefix: string | null = null,
    options: FluentClientOptions = {}
  ) {
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
    this.sendQueueFlushLimit = {
      size: +Infinity,
      length: +Infinity,
      ...(options.sendQueueFlushLimit || {}),
    };
    this.sendQueueMaxLimit = {
      size: +Infinity,
      length: +Infinity,
      ...(options.sendQueueMaxLimit || {}),
    };
    this.sendQueueNotFlushableLimit = options.sendQueueNotFlushableLimit
      ? {
          size: +Infinity,
          length: +Infinity,
          ...options.sendQueueNotFlushableLimit,
        }
      : null;

    this.socket = this.createSocket(options.security, options.socket);

    this.socket.on("writable", () => this.handleWritable());
    this.socket.on("ack", (chunkId: string) => this.handleAck(chunkId));
    if (options.onSocketError) {
      this.socket.on("error", options.onSocketError);
    }

    this.connect();
  }

  /**
   * Constructs a new socket
   *
   * @param security The security options, if any
   * @param options The socket options, if any
   * @returns A new FluentSocket
   */
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

  /**
   * Emits an event to the Fluent Server
   *
   * @param data The event to emit (required)
   * @returns A Promise, which resolves when the event is successfully sent to the server.
   *  Enabling acknowledgements waits until the server indicates they have received the event.
   */
  emit(data: protocol.EventRecord): Promise<void>;
  /**
   * Emits an event to the Fluent Server
   *
   * @param data The event to emit (required)
   * @param timestamp The timestamp of the event (optional)
   * @returns A Promise, which resolves when the event is successfully sent to the server.
   *  Enabling acknowledgements waits until the server indicates they have received the event.
   */
  emit(data: protocol.EventRecord, timestamp: Timestamp): Promise<void>;
  /**
   * Emits an event to the Fluent Server
   *
   * @param label The label to emit the data with (optional)
   * @param data The event to emit (required)
   * @returns A Promise, which resolves when the event is successfully sent to the server.
   *  Enabling acknowledgements waits until the server indicates they have received the event.
   */
  emit(label: string, data: protocol.EventRecord): Promise<void>;
  /**
   * Emits an event to the Fluent Server
   *
   * @param label The label to emit the data with (optional)
   * @param data The event to emit (required)
   * @param timestamp The timestamp of the event (optional)
   * @returns A Promise, which resolves when the event is successfully sent to the server.
   *  Enabling acknowledgements waits until the server indicates they have received the event.
   */
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
      return this.retrier.retryPromise(() => this.pushEvent(tag, time, data));
    } else {
      return this.pushEvent(tag, time, data);
    }
  }

  /**
   * Pushes an event onto the sendQueue
   *
   * Also drops items from the queue if it is too large (size/length)
   *
   * @param tag The event tag
   * @param time The event timestamp
   * @param data The event data
   * @returns The promise from the sendQueue
   */
  private pushEvent(
    tag: protocol.Tag,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void> {
    const promise = this.sendQueue.push(tag, time, data);
    this.dropLimit(this.sendQueueMaxLimit);
    this.maybeFlush();
    return promise;
  }

  /**
   * Called once the underlying socket is writable
   *
   * Should attempt a flush
   */
  private handleWritable() {
    this.maybeFlush();
  }

  /**
   * Connects the client. Happens automatically during construction, but can be called after a `disconnect()` to resume the client.
   */
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

  /**
   * Creates a tag from the passed label and the constructor `tagPrefix`.
   *
   * @param label The label to create a tag from
   * @returns The constructed tag, or `null`.
   */
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

  /**
   * Flushes to the socket internally
   *
   * Managed by `flush` to not be called multiple times
   * @returns true if there are more events in the queue to flush, false otherwise
   */
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

  /**
   * Flushes the event queue. Queues up the flushes for the next tick, preventing multiple flushes at the same time.
   *
   * @returns A promise, which resolves with a boolean indicating if there are more events to flush.
   */
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

  /**
   * Potentially triggers a flush
   *
   * If we're flushing on an interval, check if the queue (size/length) limits have been reached, and otherwise schedule a new flush
   *
   * If not, just flush
   * @returns
   */
  private maybeFlush(): void {
    // nothing to flush
    if (this.sendQueue.queueLength === 0) {
      return;
    }
    // can't flush
    if (!this.socket.writable()) {
      if (this.sendQueueNotFlushableLimit) {
        this.dropLimit(this.sendQueueNotFlushableLimit);
      }
      return;
    }
    // flush on an interval
    if (this.flushInterval > 0) {
      const limit = this.sendQueueFlushLimit;
      if (
        this.sendQueue.queueSize !== -1 &&
        this.sendQueue.queueSize >= limit.size
      ) {
        // If the queue has hit the memory flush limit
        this.flush();
      } else if (
        this.sendQueue.queueLength !== -1 &&
        this.sendQueue.queueLength >= limit.length
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

  /**
   * Drops events until the send queue is below the specified limits
   *
   * @param limit The limit to enforce
   */
  private dropLimit(limit: SendQueueLimit): void {
    if (this.sendQueue.queueSize > limit.size) {
      while (this.sendQueue.queueSize > limit.size) {
        this.sendQueue.dropEntry();
      }
    }
    if (this.sendQueue.queueLength > limit.length) {
      while (this.sendQueue.queueLength > limit.length) {
        this.sendQueue.dropEntry();
      }
    }
  }

  /**
   * Send the front item of the queue to the socket
   * @returns True if there was something to send
   */
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

  /**
   * Creates an event for how long to wait for the ack
   *
   * @param chunkId The chunk ID we're waiting to ack
   * @param deferred The deferred to reject on timeout
   * @param ackTimeout The timeout length
   * @returns
   */
  private setupAckTimeout(
    chunkId: string,
    deferred: DeferredPromise<void>,
    ackTimeout: number
  ): NodeJS.Timeout {
    return setTimeout(() => {
      // If the chunk isn't in the queue, then we must have removed it somewhere, assume that it didn't time out
      if (this.ackQueue.has(chunkId)) {
        deferred.reject(new AckTimeoutError("ack response timeout"));
        this.ackQueue.delete(chunkId);
      }
    }, ackTimeout);
  }

  /**
   * Called on an acknowledgement from the socket
   *
   * @param chunkId The chunk ID the socket has acknowledged
   * @returns
   */
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

  /**
   * Fails all acknowledgements
   * Called on shutdown
   */
  private clearAcks(): void {
    for (const data of this.ackQueue.values()) {
      clearTimeout(data.timeoutId);
      data.deferred.reject(new ShutdownError("ack queue emptied"));
    }
    this.ackQueue = new Map();
  }
}
