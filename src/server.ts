import * as net from "net";
import * as tls from "tls";
import * as crypto from "crypto";
import {EventEmitter} from "events";
import * as protocol from "./protocol";
import {UnexpectedMessageError} from "./error";

/**
 * Manages the state of the client
 */
enum ClientState {
  /**
   * Can receive events from this client
   */
  ESTABLISHED,
  /**
   * Waiting for a PING from this client
   */
  PING,
}

/**
 * The client information record
 */
type ClientInfo = {
  socket: net.Socket;
  state: ClientState;
  serverKeyInfo?: protocol.ServerKeyInfo;
  authSalt?: protocol.AuthSalt;
  sharedKeyInfo?: protocol.SharedKeyInfo;
};

type Socket = tls.TLSSocket | net.Socket;

/**
 * The server security hardening options
 */
export type FluentServerSecurityOptions = {
  /**
   * The hostname of the server. Should be unique to this process
   */
  serverHostname: string;
  /**
   * The shared key to authenticate clients with
   */
  sharedKey: string;
  /**
   * Whether to use user authentication
   */
  authorize: boolean;
  /**
   * A dict of users to their passwords
   */
  userDict: Record<string, string>;
};

/**
 * The server setup options
 */
export type FluentServerOptions = {
  /**
   * The security options.
   *
   * Defaults to undefined (no auth).
   */
  security?: FluentServerSecurityOptions;
  /**
   * Whether or not to keep the sockets alive. Sent in HELO, but currently ignored
   *
   * Defaults to false
   */
  keepalive?: boolean;
  /**
   * TLS setup options.
   *
   * See the [Node.js docs](https://nodejs.org/api/tls.html#tls_tls_createserver_options_secureconnectionlistener) for more info
   *
   * Defaults to undefined
   */
  tlsOptions?: tls.TlsOptions;
  /**
   * Socket listen options
   *
   * See the [Node.js docs](https://nodejs.org/api/net.html#net_server_listen_options_callback) for more info
   *
   * Defaults to {port: 0}
   */
  listenOptions?: net.ListenOptions;
};

/**
 * A Fluent [Forward protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1) compatible server
 */
export class FluentServer extends EventEmitter {
  private _port: number | undefined;
  private tlsOptions: tls.TlsOptions | null;
  private clients: Record<string, ClientInfo> = {};
  private server: net.Server | tls.Server;
  private security: FluentServerSecurityOptions | null;
  private keepalive: boolean;
  private listenOptions: net.ListenOptions;

  /**
   * Creates a new server
   *
   * @param options The server connection options
   */
  constructor(options: FluentServerOptions = {}) {
    super();
    this.listenOptions = options.listenOptions || {port: 0};
    this.tlsOptions = options.tlsOptions || null;
    this.security = options.security || null;
    this.clients = {};
    this.keepalive = options.keepalive || true;

    if (this.tlsOptions) {
      this.server = tls.createServer(this.tlsOptions, socket =>
        this.handleConnection(socket)
      );
    } else {
      this.server = net.createServer(socket => this.handleConnection(socket));
    }
  }

  /**
   * Handles a connection event on the server
   * @param socket
   */
  private async handleConnection(socket: Socket): Promise<void> {
    const clientKey = socket.remoteAddress + ":" + socket.remotePort;
    socket.on("end", () => {
      if (this.clients.hasOwnProperty(clientKey)) {
        delete this.clients[clientKey];
      }
    });
    const clientInfo: ClientInfo = {
      socket,
      state: ClientState.ESTABLISHED,
    };
    this.clients[clientKey] = clientInfo;
    if (this.security) {
      clientInfo.serverKeyInfo = {
        nonce: crypto.randomBytes(16),
        key: this.security.sharedKey,
      };
      if (this.security?.authorize) {
        clientInfo.authSalt = crypto.randomBytes(16);
      }
      const helo = protocol.encodeMessage(
        protocol.generateHelo(
          clientInfo.serverKeyInfo.nonce,
          clientInfo.authSalt || "",
          this.keepalive
        )
      );
      socket.write(helo);
      clientInfo.state = ClientState.PING;
    }

    // This will auto close the socket on close/decode error. We don't generally care, so this is fine
    try {
      for await (const message of protocol.decodeClientStream(socket)) {
        if (clientInfo.state === ClientState.PING && protocol.isPing(message)) {
          if (!clientInfo.serverKeyInfo || !this.security) {
            // Unexpected PONG when we didn't send a HELO
            throw new UnexpectedMessageError("Unexpected PING");
          }
          try {
            const authResult = protocol.checkPing(
              message,
              this.security.serverHostname,
              clientInfo.serverKeyInfo,
              clientInfo.authSalt
                ? {
                    salt: clientInfo.authSalt,
                    userDict: this.security.userDict,
                  }
                : undefined
            );
            clientInfo.sharedKeyInfo = authResult.sharedKeyInfo;
            const pong = protocol.generatePong(
              this.security.serverHostname,
              true,
              "",
              clientInfo.sharedKeyInfo
            );
            socket.write(protocol.encodeMessage(pong));
            clientInfo.state = ClientState.ESTABLISHED;
          } catch (e) {
            const pong = protocol.generatePong(
              this.security.serverHostname,
              false,
              (e as Error).message,
              clientInfo.sharedKeyInfo
            );
            socket.write(protocol.encodeMessage(pong));
            throw e;
          }
        } else if (
          clientInfo.state === ClientState.ESTABLISHED &&
          protocol.isClientTransportMessage(message)
        ) {
          const decodedEntries = protocol.parseTransport(message);
          if (decodedEntries.chunk) {
            const ack = protocol.encodeMessage(
              protocol.generateAck(decodedEntries.chunk)
            );
            socket.write(ack);
          }
          decodedEntries.entries.forEach(entry => {
            this.onEntry(decodedEntries.tag, ...entry);
          });
        } else {
          throw new UnexpectedMessageError("Unexpected message");
        }
      }
    } catch (e) {
      // Bad socket!
      socket.end();
    }
  }

  /**
   * Called for each entry received by the server.
   *
   * @param tag The tag of the entry
   * @param time The timestamp of the entry
   * @param record The entry record
   */
  protected onEntry(
    tag: protocol.Tag,
    time: protocol.Time,
    record: protocol.EventRecord
  ) {
    this.emit("entry", tag, time, record);
  }

  /**
   * Returns the port the server is currently listening on
   */
  get port(): number | undefined {
    return this._port;
  }

  /**
   * Start the server
   *
   * @returns A Promise which resolves once the server is listening
   */
  listen(): Promise<void> {
    return new Promise(resolve => {
      this.server.listen(this.listenOptions, () => {
        const address = this.server.address();
        this._port =
          address && typeof address === "object" ? address.port : undefined;
        resolve();
      });
    });
  }

  /**
   * Shutdown the server
   *
   * @returns A Promise, which resolves once the server has fully shut down.
   */
  close(): Promise<void> {
    return new Promise(resolve => {
      this.server.close(() => {
        resolve();
      });
      Object.values(this.clients).forEach(clientInfo =>
        clientInfo.socket.end()
      );
    });
  }
}
