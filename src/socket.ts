import * as net from "net";
import * as tls from "tls";
import * as EventEmitter from "events";
import {
  SocketTimeoutError,
  ResponseError,
  SocketNotWritableError,
  AuthError,
  FatalSocketError,
} from "./error";
import * as protocol from "./protocol";
import {PassThrough, Duplex} from "stream";

type ReconnectOptions = {
  backoff: number;
  delay: number;
  minDelay: number;
  maxDelay: number;
};

export type FluentSocketOptions = {
  path?: string;
  host?: string;
  port?: number;
  timeout?: number;
  tls?: tls.ConnectionOptions;
  reconnect?: Partial<ReconnectOptions>;
  disableReconnect?: boolean;
};

enum SocketState {
  DISCONNECTED, // No socket, (no read/write)
  CONNECTING, // Working on establishing the connection, (no read/write)
  CONNECTED, // Connected to the socket, but haven't finished connection negotiation, (internal read/write)
  ESTABLISHED, // Connected to the socket (can read/write)
  DRAINING, // The socket has blocked writes temporarily (no write, can read)
  DISCONNECTING, // The socket is in the process of being closed (no write, can read)
  IDLE, // The socket ran into an idle timeout, and will reconnect on the next write request (no read/write)
  FATAL, // The socket is misconfigured, and can't be recovered (no read/write)
}

export enum CloseState {
  FATAL, // Fatal state, socket is unusable
  CLOSE, //
  RECONNECT, // Reconnect from this state
}

const isAvailableForUserRead = (state: SocketState): boolean => {
  return (
    state === SocketState.ESTABLISHED ||
    state === SocketState.DRAINING ||
    state === SocketState.DISCONNECTING
  );
};

const isConnected = (state: SocketState): boolean => {
  return (
    state === SocketState.CONNECTED ||
    state === SocketState.ESTABLISHED ||
    state === SocketState.DRAINING ||
    state === SocketState.DISCONNECTING
  );
};

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
  private passThroughStream: PassThrough | null = null;

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

  public connect(): void {
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
        process.nextTick(() => this.connect());
      } else {
        // noop
        return;
      }
    }

    if (this.socket === null) {
      // If we're reconnecting early, then cancel the timeout
      if (this.reconnectTimeoutId !== null) {
        clearTimeout(this.reconnectTimeoutId);
        this.reconnectTimeoutId = null;
      }
      this.openSocket();
    } else if (!this.socket.writable) {
      this.disconnect();
      this.connect();
    }
  }

  private maybeReconnect(): void {
    if (!this.reconnectEnabled || this.reconnectTimeoutId !== null) {
      return;
    }
    if (this.state !== SocketState.DISCONNECTED) {
      // Socket is connected or in a fatal state
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

  private createTlsSocket(): tls.TLSSocket {
    return tls.connect({...this.tlsOptions, ...this.socketParams});
  }

  private createTcpSocket(): net.Socket {
    return net.createConnection({...this.socketParams, timeout: this.timeout});
  }

  private createSocket(onConnect: () => void): Duplex {
    if (this.tlsEnabled) {
      const socket = this.createTlsSocket();
      socket.on("secureConnect", onConnect);
      return socket;
    } else {
      const socket = this.createTcpSocket();
      socket.on("connect", onConnect);
      return socket;
    }
  }

  private openSocket(): void {
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
  }

  private handleConnect(): void {
    this.connectAttempts = 0;
    this.state = SocketState.CONNECTED;
    this.emit("connected");
    this.onConnected();
  }

  private async processMessages(
    iterable: AsyncIterable<protocol.ServerMessage>
  ): Promise<void> {
    try {
      for await (const message of iterable) {
        this.onMessage(message);
      }
    } catch (e) {
      this.close(CloseState.FATAL, e);
    }
  }

  private handleError(error: Error): void {
    this.onError(error);
  }

  private handleTimeout(): void {
    if (this.socket !== null && isConnected(this.state)) {
      this.state = SocketState.IDLE;
      this.socket.end(() => this.emit("timeout"));
    } else {
      this.close(
        CloseState.FATAL,
        new SocketTimeoutError("Socket timed out, but we weren't connected")
      );
    }
  }

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
  // @eslint-disable-next-line @typescript-eslint/no-explicit-any
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
          CloseState.CLOSE,
          new ResponseError("Received unexpected message")
        );
      }
    } else {
      this.close(
        CloseState.CLOSE,
        new ResponseError("Received unexpected message")
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
      } else if (closeState === CloseState.CLOSE) {
        this.state = SocketState.DISCONNECTING;
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

  /**
   * Write data to the socket
   *
   * Fails if the socket is not writable
   *
   * @param data The data to write to the socket
   * @returns A Promise, which resolves when the data is successfully written to the socket, or rejects if it couldn't be written
   */
  protected socketWrite(data: Uint8Array): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.socketWritable() || this.socket === null) {
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
   * Calls out to writable and socketWrite by default, but can be extended by subclasses.
   * @param data The data to write to the socket
   * @returns A Promise, which resolves when the data is successfully written to the socket, or rejects if it couldn't be written
   */
  public write(data: Uint8Array): Promise<void> {
    if (!this.writable()) {
      return Promise.reject(new SocketNotWritableError("Socket not writable"));
    }
    return this.socketWrite(data);
  }
}
