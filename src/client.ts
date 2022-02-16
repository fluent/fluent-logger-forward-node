import {
  ConfigError,
  DataTypeError,
  MissingTagError,
  AckTimeoutError,
  AckShutdownError,
} from "./error";
import EventTime from "./event_time";
import * as protocol from "./protocol";
import {FluentAuthOptions, FluentAuthSocket} from "./auth";
import {FluentSocketOptions, FluentSocket, FluentSocketEvent} from "./socket";
import {
  Queue,
  PackedForwardQueue,
  CompressedPackedForwardQueue,
  MessageQueue,
  ForwardQueue,
} from "./modes";
import * as crypto from "crypto";
import {EventRetrier, EventRetryOptions} from "./event_retrier";
import {
  DeferredPromise,
  awaitAtMost,
  awaitNextTick,
  awaitTimeout,
} from "./util";

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
const defaultLimit = (limit?: Partial<SendQueueLimit>): SendQueueLimit => {
  return {
    size: +Infinity,
    length: +Infinity,
    ...(limit || {}),
  };
};

export type DisconnectOptions = {
  /**
   * If to wait for all pending events to finish sending to the fluent server before disconnecting
   *
   * Defaults to false (does not wait)
   */
  waitForPending: boolean;
  /**
   * The maximum amount of time to wait for pending events to finish sending to the fluent server before disconnecting
   *
   * Defaults to 0 (no maximum time)
   */
  waitForPendingDelay: number;
  /**
   * The amount of time to wait before disconnecting the socket on disconnection
   *
   * Useful to wait for acknowledgements on final flush
   *
   * Defaults to 0
   */
  socketDisconnectDelay: number;
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
   * The timestamp resolution of events passed to FluentD.
   *
   * Passing false here means that the timestamps will be emitted as numbers (unless you explicitly provide an EventTime)
   * Passing true means that timestamps will alwyas be emitted as EventTime object instances. This includes timestamps
   *
   * Defaults to false (seconds). If true, the resolution will be in milliseconds.
   */
  milliseconds?: boolean;
  /**
   * How long to wait to flush the queued events
   *
   * If this is 0, we don't wait at all
   *
   * Defaults to 0
   */
  flushInterval?: number;
  /**
   * The limit at which the queue needs to be flushed.
   *
   * Used when flushInterval is > 0 to say "flush after flushInterval ms, or when the queue reaches X size"
   *
   * See the subtype for defaults
   */
  sendQueueIntervalFlushLimit?: Partial<SendQueueLimit>;
  /**
   * The limit at which we flush synchronously. By default, we flush asynchronously,
   * which can be bad if we're sending 30k+ events at a time.
   *
   * This sets a size limit at which we flush synchronously within emit(), which makes
   * sure we're flushing as quickly as possible
   *
   * Defaults to null (no limit)
   *
   * See the subtype for defaults
   */
  sendQueueSyncFlushLimit?: Partial<SendQueueLimit>;
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
   * The delay after which we're not writable to start flushing events.
   * Useful to make sure we don't drop events during short blips
   *
   * Defaults to 0 (no delay)
   */
  sendQueueNotFlushableLimitDelay?: number;
  /**
   * Retry event submission on failure
   *
   * Warning: This effectively keeps the event in memory until it is successfully sent or retries exhausted
   *
   * See subtype for defaults
   */
  eventRetry?: Partial<EventRetryOptions>;
  /**
   * Options to control disconnection behavior
   *
   * How many times to try to flush before disconnecting, wait times, etc
   *
   * See subtype for defaults
   */
  disconnect?: Partial<DisconnectOptions>;
  /**
   * Disable connection on client creation. Expects the client to call .connect to start sending messages.
   *
   * Defaults to false
   */
  disableAutoconnect?: boolean;
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
  private emitQueue: Set<Promise<void>> = new Set();
  private milliseconds: boolean;
  private retrier: EventRetrier | null;

  private socket: FluentSocket;

  private flushInterval: number;
  private sendQueueIntervalFlushLimit: SendQueueLimit | null;
  private sendQueueSyncFlushLimit: SendQueueLimit | null;
  private sendQueueMaxLimit: SendQueueLimit | null;
  private sendQueueNotFlushableLimit: SendQueueLimit | null;
  private sendQueueNotFlushableLimitDelay: number;

  private notFlushableLimitTimeoutId: null | NodeJS.Timeout = null;
  private nextFlushTimeoutId: null | NodeJS.Timeout = null;
  private flushing = false;
  private willFlushNextTick: Promise<boolean> | null = null;
  private disconnectOptions: DisconnectOptions;

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
    this.milliseconds = !!options.milliseconds;

    this.flushInterval = options.flushInterval || 0;
    this.sendQueueSyncFlushLimit = options.sendQueueSyncFlushLimit
      ? defaultLimit(options.sendQueueSyncFlushLimit)
      : null;
    this.sendQueueIntervalFlushLimit = options.sendQueueIntervalFlushLimit
      ? defaultLimit(options.sendQueueIntervalFlushLimit)
      : null;
    this.sendQueueMaxLimit = options.sendQueueMaxLimit
      ? defaultLimit(options.sendQueueMaxLimit)
      : null;
    this.sendQueueNotFlushableLimit = options.sendQueueNotFlushableLimit
      ? defaultLimit(options.sendQueueNotFlushableLimit)
      : null;
    this.sendQueueNotFlushableLimitDelay =
      options.sendQueueNotFlushableLimitDelay || 0;

    this.disconnectOptions = {
      waitForPending: false,
      waitForPendingDelay: 0,
      socketDisconnectDelay: 0,
      ...(options.disconnect || {}),
    };

    this.socket = this.createSocket(options.security, options.socket);

    this.socket.on(FluentSocketEvent.WRITABLE, () => this.handleWritable());
    this.socket.on(FluentSocketEvent.ACK, (chunkId: string) =>
      this.handleAck(chunkId)
    );

    // Only connect if we're able to reconnect and user has not disabled auto connect
    // Otherwise we expect an explicit connect() which will handle connection errors
    const autoConnect =
      !options.socket?.disableReconnect && !options.disableAutoconnect;
    if (autoConnect) {
      // Catch errors and noop them, so the constructor doesn't throw unhandled promises
      // They can be handled by the socket "error" event handler anyway
      this.connect().catch(() => {});
    }
  }

  /**
   * Attaches an event listener to the underlying socket
   *
   * See FluentSocketEvent for more info
   */
  public socketOn(
    event: FluentSocketEvent,
    listener: (...args: any[]) => void // eslint-disable-line @typescript-eslint/no-explicit-any
  ): void {
    this.socket.on(event, listener);
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
   * @param timestamp A millisecond resolution timestamp to associate with the event (optional)
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
   * @param timestamp A millisecond resolution timestamp to associate with the event (optional)
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

    let millisOrEventTime: number | EventTime;
    if (timestamp === null || timestamp instanceof Date) {
      millisOrEventTime = timestamp ? timestamp.getTime() : Date.now();
    } else {
      millisOrEventTime = timestamp;
    }

    let time: protocol.Time;
    if (typeof millisOrEventTime === "number") {
      // Convert timestamp to EventTime or number in second resolution
      time = this.milliseconds
        ? EventTime.fromTimestamp(millisOrEventTime)
        : Math.floor(millisOrEventTime / 1000);
    } else {
      time = millisOrEventTime;
    }
    let emitPromise: Promise<void>;
    if (this.retrier !== null) {
      emitPromise = this.retrier.retryPromise(() =>
        this.pushEvent(tag, time, data)
      );
    } else {
      emitPromise = this.pushEvent(tag, time, data);
    }
    if (!this.emitQueue.has(emitPromise)) {
      this.emitQueue.add(emitPromise);
      emitPromise
        .finally(() => this.emitQueue.delete(emitPromise))
        .catch(() => {});
    }
    return emitPromise;
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
    if (this.sendQueueMaxLimit) {
      this.dropLimit(this.sendQueueMaxLimit);
    }
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
   * Connects the client. Can happen automatically during construction, but can be called after a `disconnect()` to resume the client.
   */
  public async connect(): Promise<void> {
    await this.socket.connect();
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
      // Flush before awaiting
      await this.flush();
      if (this.disconnectOptions.waitForPending) {
        const flushPromise = this.waitForPending();
        if (this.disconnectOptions.waitForPendingDelay > 0) {
          await awaitAtMost(
            flushPromise,
            this.disconnectOptions.waitForPendingDelay
          );
        } else {
          await flushPromise;
        }
      }
    } finally {
      if (this.disconnectOptions.socketDisconnectDelay > 0) {
        await awaitTimeout(this.disconnectOptions.socketDisconnectDelay);
      }
      try {
        await this.socket.disconnect();
      } finally {
        // We want to client to be in a state where nothing is pending that isn't in the sendQueue, now that the socket is unflushable.
        // This means nothing is pending acknowledgemnets, and nothing is pending to retry.
        // As a result, we can drop all the pending events, or send them once we're connected again
        // Drop the acks first, as they can queue up retries which we need to short circuit
        await this.clearAcks();
        if (this.retrier) {
          // Short circuit all retries, so they requeue immediately
          await this.retrier.shortCircuit();
        }
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
   * Flushes to the socket synchronously
   *
   * Prefer calling `.flush` which will flush on the next tick, allowing events from this tick to queue up.
   *
   * @returns true if there are more events in the queue to flush, false otherwise
   */
  public syncFlush(): boolean {
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
          resolve(this.syncFlush());
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
      if (
        this.sendQueueNotFlushableLimit &&
        this.notFlushableLimitTimeoutId === null
      ) {
        if (this.sendQueueNotFlushableLimitDelay > 0) {
          this.notFlushableLimitTimeoutId = setTimeout(() => {
            this.notFlushableLimitTimeoutId = null;
            if (this.sendQueueNotFlushableLimit) {
              this.dropLimit(this.sendQueueNotFlushableLimit);
            }
          }, this.sendQueueNotFlushableLimitDelay);
        } else {
          this.dropLimit(this.sendQueueNotFlushableLimit);
        }
      }
      return;
    } else {
      // When writable, we want to clear the not flushable limit
      if (this.notFlushableLimitTimeoutId !== null) {
        clearTimeout(this.notFlushableLimitTimeoutId);
        this.notFlushableLimitTimeoutId = null;
      }
    }
    // If we've hit a blocking limit
    if (
      this.sendQueueSyncFlushLimit &&
      this.shouldLimit(this.sendQueueSyncFlushLimit)
    ) {
      this.syncFlush();
    } else if (this.flushInterval > 0) {
      if (
        this.sendQueueIntervalFlushLimit &&
        this.shouldLimit(this.sendQueueIntervalFlushLimit)
      ) {
        this.flush();
      } else if (this.nextFlushTimeoutId === null) {
        // Otherwise, schedule the next flush interval
        this.nextFlushTimeoutId = setTimeout(() => {
          this.nextFlushTimeoutId = null;
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
    if (
      this.sendQueue.queueSize !== -1 &&
      this.sendQueue.queueSize > limit.size
    ) {
      while (this.sendQueue.queueSize > limit.size) {
        this.sendQueue.dropEntry();
      }
    }
    if (
      this.sendQueue.queueLength !== -1 &&
      this.sendQueue.queueLength > limit.length
    ) {
      while (this.sendQueue.queueLength > limit.length) {
        this.sendQueue.dropEntry();
      }
    }
  }

  /**
   * Checks if the sendQueue hits this limit
   * @param limit the limit to check
   */
  private shouldLimit(limit: SendQueueLimit): boolean {
    if (
      this.sendQueue.queueSize !== -1 &&
      this.sendQueue.queueSize >= limit.size
    ) {
      // If the queue has hit the memory flush limit
      return true;
    } else if (
      this.sendQueue.queueLength !== -1 &&
      this.sendQueue.queueLength >= limit.length
    ) {
      // If the queue has hit the length flush limit
      return true;
    }
    return false;
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
      writePromise.then(
        () => nextPacket.deferred.resolve(),
        () => {}
      );
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
   *
   * @returns a Promise which resolves once all the handlers depending on the ack result have resolved
   */
  private async clearAcks(): Promise<void> {
    for (const data of this.ackQueue.values()) {
      clearTimeout(data.timeoutId);
      data.deferred.reject(new AckShutdownError("ack queue emptied"));
    }
    this.ackQueue = new Map();

    // We want this to resolve on the next tick, once handlers depending on the ack result have fully resolved
    // i.e we have emptied PromiseJobs
    return awaitNextTick();
  }

  /**
   * Returns the number of queued events that haven't been sent yet
   *
   * Useful to react if we're queuing up too many events within a single tick
   */
  get sendQueueLength(): number {
    return this.sendQueue.queueLength;
  }

  /**
   * Returns whether or not the socket is writable
   *
   * Useful to react if we're disconnected for any reason
   */
  get writable(): boolean {
    return this.socket.writable();
  }

  /**
   * Returns the number of events that have been queued, but haven't resolved yet
   *
   * This includes acknowledgements and retries if enabled.
   */
  get queueLength(): number {
    return this.emitQueue.size;
  }

  /**
   * Waits for all currently pending events to successfully resolve or reject
   *
   * @returns A Promise which resolves once all the pending events have successfully been emitted
   */
  public async waitForPending(): Promise<void> {
    // Clone the emitQueue, to ignore emit calls made while waiting
    await Promise.allSettled(Array.from(this.emitQueue));
  }
}
