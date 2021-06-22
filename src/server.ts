import * as net from "net";
import * as tls from "tls";
import * as crypto from "crypto";
import * as EventEmitter from "events";
import * as protocol from "./protocol";
import {ResponseError} from "./error";

enum ClientState {
  ESTABLISHED,
  HELO,
  PING,
}

type ClientInfo = {
  socket: net.Socket;
  state: ClientState;
  serverKeyInfo?: protocol.ServerKeyInfo;
  authSalt?: protocol.AuthSalt;
  sharedKeyInfo?: protocol.SharedKeyInfo;
};

type Socket = tls.TLSSocket | net.Socket;

type FluentServerSecurityOptions = {
  serverHostname: string;
  sharedKey: string;
  authorize: boolean;
  userDict: Record<string, string>;
};

type FluentServerOptions = {
  security?: FluentServerSecurityOptions;
  keepalive?: boolean;
  tlsOptions?: tls.TlsOptions;
  listenOptions?: net.ListenOptions;
};

export class FluentServer extends EventEmitter {
  private _port: number | undefined;
  private tlsOptions: tls.TlsOptions | null;
  private clients: Record<string, ClientInfo> = {};
  private server: net.Server | tls.Server;
  private security: FluentServerSecurityOptions | null;
  private keepalive: boolean;
  private listenOptions: net.ListenOptions;

  constructor(options: FluentServerOptions = {}) {
    super();
    this.listenOptions = options.listenOptions || { port: 0 };
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
            throw new ResponseError("Unexpected PING");
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
              e.message,
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
          throw new ResponseError("Unexpected message");
        }
      }
    } catch (e) {
      // Bad socket!
      socket.end();
    }
  }

  onEntry(tag: string, time: protocol.Time, record: protocol.EventRecord) {
    this.emit("entry", tag, time, record);
  }

  get port() {
    return this._port;
  }

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
