import * as crypto from "crypto";
import {FluentSocket, FluentSocketOptions, CloseState} from "./socket";
import {UnexpectedMessageError} from "./error";
import * as protocol from "./protocol";

/**
 * The authentication options for the client
 */
export type FluentAuthOptions = {
  /**
   * The client host name (required).
   *
   * Must be unique to this process
   */
  clientHostname: string;
  /**
   * The shared key with the server. (required)
   */
  sharedKey: string;
  /**
   * The username to authenticate with. (optional)
   */
  username?: string;
  /**
   * The password to authenticate with. (optional)
   */
  password?: string;
};

enum FluentAuthState {
  /**
   * The client is not authenticated (socket is not connected)
   */
  UNAUTHENTICATED,
  /**
   * The client is waiting for a HELO from the server
   */
  HELO,
  /**
   * The client is waiting for a PONG from the server
   */
  PONG,
  /**
   * The client is fully authenticated
   */
  AUTHENTICATED,
}

/**
 * An implementation of FluentSocket which authenticates the socket using the [Forward protocol Handshake](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#handshake-messages)
 */
export class FluentAuthSocket extends FluentSocket {
  private authState: FluentAuthState = FluentAuthState.UNAUTHENTICATED;
  private sharedKeyInfo: protocol.SharedKeyInfo;
  private authInfo?: protocol.ClientAuthInfo;
  private clientHostname: string;
  private username: string;
  private password: string;

  /**
   * Creates a new instance of the socket
   * @param authOptions The authentication options to use
   * @param socketOptions The socket options to pass to the underlying socket
   */
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

  /**
   * Once the socket is connected, we expect a HELO
   */
  protected onConnected(): void {
    this.authState = FluentAuthState.HELO;
  }

  /**
   * When the socket is closed, we're unauthenticated
   */
  protected onClose(): void {
    this.authState = FluentAuthState.UNAUTHENTICATED;
    super.onClose();
  }

  /**
   * Handles messages from the server
   *
   * If we're waiting for a message, this will trigger it, otherwise just forward to the superclass.
   *
   * @param message The message to check
   */
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
      this.close(
        CloseState.FATAL,
        new UnexpectedMessageError("Received unexpected message")
      );
    }
  }

  /**
   * Called on a HELO message
   *
   * Should parse the message, and send back a PING
   *
   * @param message The HELO message
   */
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
    this.socketWrite(ping).catch(err => this.close(CloseState.RECONNECT, err));
  }

  /**
   * Called on a PONG message
   *
   * Should parse and validate the message, and if valid, establish the connection
   *
   * @param message The PONG message
   * @returns void
   */
  private handlePong(message: protocol.PongMessage): void {
    try {
      protocol.checkPong(message, this.clientHostname, this.sharedKeyInfo);
      this.authState = FluentAuthState.AUTHENTICATED;
      this.onEstablished();
    } catch (e) {
      return this.close(CloseState.FATAL, e as Error);
    }
  }
}
