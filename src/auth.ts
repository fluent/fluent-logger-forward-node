import * as crypto from "crypto";
import {FluentSocket, FluentSocketOptions} from "./socket";
import {ResponseError} from "./error";
import * as protocol from "./protocol";

export type FluentAuthOptions = {
  clientHostname: string;
  sharedKey: string;
  username?: string;
  password?: string;
};

enum FluentAuthState {
  UNAUTHENTICATED,
  HELO,
  PONG,
  AUTHENTICATED,
}

export class FluentAuthSocket extends FluentSocket {
  private authState: FluentAuthState = FluentAuthState.UNAUTHENTICATED;
  private sharedKeyInfo: protocol.SharedKeyInfo;
  private authInfo?: protocol.ClientAuthInfo;
  private clientHostname: string;
  private username: string;
  private password: string;

  constructor(
    authOptions: FluentAuthOptions,
    socketOptions: FluentSocketOptions = {}
  ) {
    super(socketOptions);
    this.clientHostname = authOptions.clientHostname;
    this.sharedKeyInfo = {
      salt: crypto.randomBytes(16).toString("hex"),
      nonce: "",
      key: authOptions.sharedKey,
    };
    this.username = authOptions.username || "";
    this.password = authOptions.password || "";
  }

  protected onConnected(): void {
    this.authState = FluentAuthState.HELO;
  }

  protected onClose(): void {
    this.authState = FluentAuthState.UNAUTHENTICATED;
    super.onClose();
  }

  protected onMessage(message: protocol.ServerMessage): void {
    if (protocol.isHelo(message) && this.authState === FluentAuthState.HELO) {
      this.handleHelo(message);
    } else if (
      protocol.isPong(message) &&
      this.authState === FluentAuthState.PONG
    ) {
      this.handlePong(message);
    } else if (this.authState === FluentAuthState.AUTHENTICATED) {
      super.onMessage(message);
    } else {
      this.close(new ResponseError("Received unexpected message"));
    }
  }

  private handleHelo(message: protocol.HeloMessage): void {
    const heloOptions = protocol.parseHelo(message);
    this.authState = FluentAuthState.PONG;

    this.sharedKeyInfo.nonce = heloOptions.nonce;
    if (heloOptions.auth) {
      this.authInfo = {
        username: this.username,
        password: this.password,
        salt: heloOptions.auth,
      };
    }
    // Write to the socket directly (bypass writable() below)
    const ping = protocol.encodeMessage(
      protocol.generatePing(
        this.clientHostname,
        this.sharedKeyInfo,
        this.authInfo
      )
    );
    this.socketWrite(ping).catch(err => this.closeAndReconnect(err));
  }

  private handlePong(message: protocol.PongMessage): void {
    try {
      protocol.checkPong(message, this.clientHostname, this.sharedKeyInfo);
      this.authState = FluentAuthState.AUTHENTICATED;
      this.onEstablished();
    } catch (e) {
      return this.close(e);
    }
  }
}
