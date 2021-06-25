import * as net from "net";
import * as tls from "tls";
import * as EventEmitter from "events";
import {
  SocketTimeoutError,
  UnexpectedMessageError,
  SocketNotWritableError,
  AuthError,
  FatalSocketError,
} from "./error";
import * as protocol from "./protocol";
import {PassThrough, Duplex} from "stream";

/**
 * Reconnection settings for the socket
 *
 * The parameters represent an exponential backoff formula:
 * min(maxDelay, max(minDelay, backoff^attempts * delay))
 *
 * The attempt count is incremented each time we fail a connection,
 * and set to zero each time a connection is successfully made.
 * Note this is before handshaking
 */
export type ReconnectOptions = {
  /**
   * The backoff factor for each attempt
   *
   * Defaults to 2
   */
  backoff: number;
  /**
   * The delay factor for each attempt
   *
   * Defaults to 500
   */
  delay: number;
  /**
   * The global minimum delay
   */
  minDelay: number;
  /**
   * The global maximum delay
   */
  maxDelay: number;
};

export type FluentSocketOptions = {
  /**
   * If connecting to a unix domain socket, e.g unix:///var/run/fluentd.sock, then specify that here.
   *
   * This overrides host/port below. Defaults to `undefined`.
   */
  path?: string;
  /**
   * The host (IP) to connect to
   *
   * Defaults to `localhost`.
   */
  host?: string;
  /**
   * The port to connect to
   *
   * Defaults to `24224`.
   */
  port?: number;
  /**
   * The socket timeout to set. After timing out, the socket will be idle and reconnect once the client wants to write something.
   *
   * Defaults to 3000 (3 seconds)
   */
  timeout?: number;
  /**
   * TLS connection options. See [Node docs](https://nodejs.org/api/tls.html#tls_tls_connect_options_callback)
   *
   * If provided, the socket will be a TLS socket.
   */
  tls?: tls.ConnectionOptions;
  /**
   * Reconnection options. See subtype for defaults.
   */
  reconnect?: Partial<ReconnectOptions>;
  /**
   * Disable reconnection on failure. This can be useful for one-offs
   *
   * Defaults to false
   */
  disableReconnect?: boolean;
};

enum SocketState {
  /**
   * In this state, the socket doesn't exist, and we can't read or write from it.
   *
   * No read
   * No write
   * Transitions to CONNECTING on call to connect() (potentially from maybeReconnect())
   */
  DISCONNECTED,
  /**
   * In this state we're working on making the connection (opening socket + TLS negotiations if any), but haven't finished.
   *
   * No read
   * No write
   * Transitions to DISCONNECTED on error
   * Transitions to CONNECTED on success
   */
  CONNECTING,
  /**
   * In this state, we're doing some preparatory work before accepting writes
   *
   * Internal read
   * Internal write
   * Transitions to DISCONNECTED on soft error (will reconnect)
   * Transitions to DISCONNECTING on close + medium error
   * Transitions to FATAL on hard error
   */
  CONNECTED,
  /**
   * In this state, we're fully open, and able to read and write to the socket
   *
   * Can read
   * Can write
   * Transitions to DISCONNECTED on soft error (will reconnect)
   * Transitions to DISCONNECTING on close + medium error
   * Transitions to FATAL on hard error
   * Tansitions to DRAINING when socket.write returns false (buffer full)
   * Transitions to IDLE on timeout
   */
  ESTABLISHED,
  /**
   * In this state, the socket has blocked writes, as the kernel buffer is full.
   *
   * Can read
   * No write
   * Transitions to ESTABLISHED on drain event
   * Transitions to DISCONNECTED on soft error (will reconnect)
   * Transitions to DISCONNECTING on close + medium error
   * Transitions to FATAL on hard error
   */
  DRAINING,
  /**
   * In this state, the socket is being closed, and will not be reconnected, either as the result of user action, or an event.
   *
   * Can read
   * No write
   * Transitions to DISCONNECTED on close event
   */
  DISCONNECTING, // The socket is in the process of being closed (no write, can read)
  /**
   * In this state, the socket has timed out due to inactivity. It will be reconnected once the user calls `writable()`.
   *
   * We don't auto reconnect from this state, as the idle timeout indicates low event activity.
   * It can also potentially indicate a misconfiguration where the timeout is too low.
   *
   * No read
   * No write
   * Transitions to CONNECTING on call to connect() (potentially from writable())
   */
  IDLE,
  /**
   * In this state, the socket has run into a fatal error, which it believes there is no point in reconnecting.
   *
   * This is almost always a configuration misconfiguration, for example the server requires auth, but the client has no auth information.
   *
   * No read
   * No write
   * Does not transition
   */
  FATAL,
}

/**
 * How to close the socket
 */
export enum CloseState {
  /**
   * Make the socket unable to reconnect
   */
  FATAL,
  /**
   * Allow the socket to reconnect automatically
   */
  RECONNECT,
}

const isAvailableForUserRead = (state: SocketState): boolean => {
  return (
    state === SocketState.ESTABLISHED ||
    state === SocketState.DRAINING ||
    state === SocketState.DISCONNECTING
  );
};

/**
 * A wrapper around a Fluent Socket
 *
 * Handles connecting the socket, and manages error events and reconnection
 */
export class FluentSocket extends EventEmitter {
  private state: SocketState = SocketState.DISCONNECTED;
  private socket: Duplex | null = null;

  private reconnectTimeoutId: null | NodeJS.Timeout = null;
  private connectAttempts = 0;

  private socketParams: net.TcpSocketConnectOpts | net.IpcSocketConnectOpts;
  private timeout: number;
  private tlsEnabled: boolean;
  private tlsOptions: tls.ConnectionOptions;
  private reconnectEnabled: boolean;
  private reconnect: ReconnectOptions;
  /**
   * Used so we can read from the socket through an AsyncIterable.
   * Protects the reader from accidentally closing the socket on errors.
   */
  private passThroughStream: PassThrough | null = null;

  /**
   * Creates a new socket
   *
   * @param options The socket connection options
   */
  constructor(options: FluentSocketOptions = {}) {
    super();
    if (options.path) {
      this.socketParams = {path: options.path};
    } else {
      this.socketParams = {
        host: options.host || "localhost",
        port: options.port || 24224,
      };
    }

    this.timeout = options.timeout || 3000;

    this.tlsEnabled = !!options.tls;
    this.tlsOptions = options.tls || {};

    this.reconnectEnabled = !options.disableReconnect;
    this.reconnect = {
      backoff: 2,
      delay: 500, // default is 500ms
      minDelay: -Infinity,
      maxDelay: +Infinity,
      ...(options.reconnect || {}),
    };
  }

  /**
   * Connects the socket to the upstream server
   *
   * @returns void
   */
  public async connect(): Promise<void> {
    if (this.state === SocketState.FATAL) {
      throw new FatalSocketError(
        "Socket is fatally closed, create a new socket to reconnect"
      );
    }
    if (
      this.state !== SocketState.DISCONNECTED &&
      this.state !== SocketState.IDLE
    ) {
      if (this.state === SocketState.DISCONNECTING) {
        // Try again once the socket has fully closed
        await new Promise(r => process.nextTick(r));
        return await this.connect();
      } else {
        // noop, we're connected
        return;
      }
    }

    if (this.socket === null) {
      // If we're reconnecting early, then cancel the timeout
      if (this.reconnectTimeoutId !== null) {
        clearTimeout(this.reconnectTimeoutId);
        this.reconnectTimeoutId = null;
      }
      try {
        await this.openSocket();
      } catch (e) {
        if (!this.reconnectEnabled) {
          throw e;
        } else {
          // Suppress connection errors if reconnections are enabled
          return;
        }
      }
    } else if (!this.socket.writable) {
      await this.disconnect();
      await this.connect();
    }
  }

  /**
   * May reconnect the socket
   * @returns void
   */
  private maybeReconnect(): void {
    if (!this.reconnectEnabled || this.reconnectTimeoutId !== null) {
      return;
    }
    if (this.state !== SocketState.DISCONNECTED) {
      // Socket is connected or in a fatal state or idle
      return;
    }
    // Exponentially back off based on this.connectAttempts
    const reconnectInterval = Math.min(
      this.reconnect.maxDelay,
      Math.max(
        this.reconnect.minDelay,
        this.reconnect.backoff ** this.connectAttempts * this.reconnect.delay
      )
    );

    this.reconnectTimeoutId = setTimeout(() => {
      this.reconnectTimeoutId = null;
      this.connect();
    }, reconnectInterval);
  }

  /**
   * Creates a new TLS socket
   * @returns A new socket to use for the connection
   */
  private createTlsSocket(): tls.TLSSocket {
    return tls.connect({...this.tlsOptions, ...this.socketParams});
  }

  /**
   * Creates a new TCP socket
   * @returns A new socket to use for the connection
   */
  private createTcpSocket(): net.Socket {
    return net.createConnection({...this.socketParams, timeout: this.timeout});
  }

  /**
   * Returns a new socket
   *
   * @param onConnect Called once the socket is connected
   * @returns
   */
  private createSocket(onConnect: () => void): Duplex {
    if (this.tlsEnabled) {
      const socket = this.createTlsSocket();
      socket.once("secureConnect", onConnect);
      return socket;
    } else {
      const socket = this.createTcpSocket();
      socket.once("connect", onConnect);
      return socket;
    }
  }

  /**
   * Sets up and connects the socket
   *
   * @returns A promise which resolves once the socket is connected, or once it is errored
   */
  private openSocket(): Promise<void> {
    this.state = SocketState.CONNECTING;
    this.socket = this.createSocket(() => this.handleConnect());

    this.socket.on("error", err => this.handleError(err));
    this.socket.on("timeout", () => this.handleTimeout());
    this.socket.on("close", () => this.handleClose());
    this.socket.on("drain", () => this.handleDrain());

    // Pipe through a passthrough stream before passing to msgpack
    // This prevents error events on the socket from affecting the decode pipeline
    this.passThroughStream = new PassThrough();
    this.socket.pipe(this.passThroughStream);
    this.processMessages(protocol.decodeServerStream(this.passThroughStream));

    return new Promise<void>((resolve, reject) => {
      // This may call both resolve and reject, but the ES standard says this is OK
      this.once("connected", () => resolve());
      this.once("error", err => reject(err));
    });
  }

  /**
   * Called once the socket is connected
   */
  private handleConnect(): void {
    this.connectAttempts = 0;
    this.state = SocketState.CONNECTED;
    this.emit("connected");
    this.onConnected();
  }

  /**
   * Processes messages from the socket
   *
   * @param iterable The socket read data stream
   * @returns Promise for when parsing completes
   */
  private async processMessages(
    iterable: AsyncIterable<protocol.ServerMessage>
  ): Promise<void> {
    try {
      for await (const message of iterable) {
        this.onMessage(message);
      }
    } catch (e) {
      this.close(CloseState.RECONNECT, e);
    }
  }

  /**
   * Called from an error event on the socket
   */
  private handleError(error: Error): void {
    if ((error as NodeJS.ErrnoException).code === "ECONNRESET") {
      // This is OK in disconnecting states
      if (
        this.state === SocketState.DISCONNECTING ||
        this.state === SocketState.IDLE ||
        this.state === SocketState.DISCONNECTED
      ) {
        return;
      }
    }
    this.onError(error);
  }

  /**
   * Called when the socket times out
   * Should suspend the socket (set it to IDLE)
   */
  private handleTimeout(): void {
    if (this.socket !== null) {
      this.state = SocketState.IDLE;
      this.socket.end(() => this.emit("timeout"));
    } else {
      this.close(
        CloseState.FATAL,
        new SocketTimeoutError("Socket timed out, but socket wasn't open")
      );
    }
  }

  /**
   * Called from a "close" event on the socket
   *
   * Should clean up the state, and potentially trigger a reconnect
   */
  private handleClose(): void {
    if (this.state === SocketState.CONNECTING) {
      // If we never got to the CONNECTED stage
      // Prevents us from exponentially retrying configuration errors
      this.connectAttempts += 1;
    }

    this.socket = null;
    // Make sure the passthrough stream is closed
    this.passThroughStream?.end();
    this.passThroughStream = null;

    let triggerReconnect = false;
    // Only try to reconnect if we had an didn't expect to disconnect or hit a fatal error
    if (this.state !== SocketState.FATAL && this.state !== SocketState.IDLE) {
      if (this.state !== SocketState.DISCONNECTING) {
        triggerReconnect = true;
      }
      this.state = SocketState.DISCONNECTED;
    }
    this.onClose();

    if (triggerReconnect) {
      this.maybeReconnect();
    }
  }

  /**
   * Called when the socket has fully drained, and the buffers are free again
   */
  private handleDrain(): void {
    // We may not have noticed that we were draining, or we may have moved to a different state in the mean time
    if (this.state === SocketState.DRAINING) {
      this.state = SocketState.ESTABLISHED;
      this.emit("drain");
      this.onWritable();
    }
  }

  /**
   * Handles a connection event on the connection
   *
   * Called once a connection is established
   */
  protected onConnected(): void {
    this.onEstablished();
  }

  /**
   * Called once a connection is ready to accept writes externally
   */
  protected onEstablished(): void {
    this.state = SocketState.ESTABLISHED;
    this.emit("established");
    this.onWritable();
  }

  /**
   * Called once we think socket.writable() will return true
   * Note that this event doesn't guarantee that socket.writable() will return true,
   * for example, the server might disconnect in between emitting the event and attempting a write.
   */
  protected onWritable(): void {
    this.emit("writable");
  }

  /**
   * Handles an error event on the connection
   *
   * @param error The error
   */
  protected onError(error: Error): void {
    this.emit("error", error);
  }

  /**
   * Handles a close event from the socket
   */
  protected onClose(): void {
    this.emit("close");
  }

  // This is the EventEmitter signature
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public emit(event: string | symbol, ...args: any[]): boolean {
    if (this.listenerCount(event) > 0) {
      return super.emit(event, ...args);
    } else {
      return false;
    }
  }

  /**
   * Handles a message from the server
   *
   * @param message The decoded message
   */
  protected onMessage(message: protocol.ServerMessage): void {
    if (isAvailableForUserRead(this.state)) {
      if (protocol.isAck(message)) {
        this.onAck(message.ack);
      } else if (protocol.isHelo(message)) {
        this.close(
          CloseState.FATAL,
          new AuthError(
            "Server expected authentication, but client didn't provide any, closing"
          )
        );
      } else {
        this.close(
          CloseState.FATAL,
          new UnexpectedMessageError("Received unexpected message")
        );
      }
    } else {
      this.close(
        CloseState.FATAL,
        new UnexpectedMessageError("Received unexpected message")
      );
    }
  }

  /**
   * Handle an ack from the server
   *
   * @param chunkId The chunk from the ack event
   */
  protected onAck(chunkId: protocol.Chunk) {
    this.emit("ack", chunkId);
  }

  /**
   * Gracefully closes the connection
   *
   * Changes state to DISCONNECTING, meaning we don't reconnect from this state
   */
  public disconnect(): Promise<void> {
    return new Promise(resolve => {
      if (this.socket !== null) {
        this.state = SocketState.DISCONNECTING;
        this.socket.end(resolve);
      } else {
        resolve();
      }
    });
  }

  /**
   * Forcefully closes the connection, and optionally emits an error
   *
   * Changes state to DISCONNECTING, meaning we don't reconnect from this state
   * @param closeState The state to close this socket in
   * @param error The error that closed the socket
   */
  public close(closeState: CloseState, error?: Error): void {
    if (this.socket !== null) {
      if (closeState === CloseState.FATAL) {
        this.state = SocketState.FATAL;
      }
      this.socket.destroy();
    }
    if (error) {
      this.onError(error);
    }
  }

  /**
   * Check if the socket is writable
   *
   * Will terminate the socket if it is half-closed
   *
   * Will connect the socket if it is disconnected
   * @returns If the socket is in a state to be written to
   */
  private socketWritable(): boolean {
    // Accept CONNECTED and ESTABLISHED as writable states
    if (
      this.socket === null ||
      (this.state !== SocketState.ESTABLISHED &&
        this.state !== SocketState.CONNECTED)
    ) {
      // Resume from idle state
      if (this.state === SocketState.IDLE) {
        this.connect();
      }
      return false;
    }
    // Check if the socket is writable
    if (!this.socket.writable) {
      this.close(
        CloseState.RECONNECT,
        new SocketNotWritableError("Socket not writable")
      );
      return false;
    }
    return true;
  }

  /**
   * Check if the socket is writable
   *
   * @returns If the socket is in a state to be written to
   */
  public writable(): boolean {
    return this.socketWritable() && this.state === SocketState.ESTABLISHED;
  }

  private innerWrite(data: Uint8Array): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.socket === null) {
        return reject(new SocketNotWritableError("Socket not writable"));
      }
      const keepWriting = this.socket.write(data, err => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
      if (!keepWriting) {
        this.state = SocketState.DRAINING;
      }
    });
  }

  /**
   * Write data to the socket
   *
   * Fails if the socket is not writable
   *
   * @param data The data to write to the socket
   * @returns A Promise, which resolves when the data is successfully written to the socket, or rejects if it couldn't be written
   */
  protected socketWrite(data: Uint8Array): Promise<void> {
    if (!this.socketWritable()) {
      return Promise.reject(new SocketNotWritableError("Socket not writable"));
    }
    return this.innerWrite(data);
  }

  /**
   * Write data to the socket
   *
   * Fails if the socket is not writable
   *
   * @param data The data to write to the socket
   * @returns A Promise, which resolves when the data is successfully written to the socket, or rejects if it couldn't be written
   */
  public write(data: Uint8Array): Promise<void> {
    if (!this.writable()) {
      return Promise.reject(new SocketNotWritableError("Socket not writable"));
    }
    return this.innerWrite(data);
  }
}
