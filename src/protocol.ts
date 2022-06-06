/* eslint-disable @typescript-eslint/no-explicit-any */

import * as crypto from "crypto";
import * as zlib from "zlib";
import EventTime from "./event_time";
import {Encoder, Decoder, ExtensionCodec} from "@msgpack/msgpack";
import {Readable} from "stream";
import {
  ConfigError,
  AuthError,
  SharedKeyMismatchError,
  DecodeError,
} from "./error";

// Types from the [Forward protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1)

export type SharedKey = string;
export type SharedKeyNonce = string | Uint8Array;
export type AuthSalt = string | Uint8Array;
export type Keepalive = boolean;

export type HeloOptions = {
  nonce: SharedKeyNonce;
  auth?: AuthSalt;
  keepalive?: Keepalive;
};

export type HeloMessage = ["HELO", HeloOptions];

export type ClientHostname = string;
export type SharedKeySalt = string | Uint8Array;
export type SharedKeyHexDigest = string;
export type Username = string;
export type Password = string;
export type PasswordDigest = string;

export type PingMessage = [
  "PING",
  ClientHostname,
  SharedKeySalt,
  SharedKeyHexDigest,
  Username,
  Password
];

export type Authenticated = boolean;
export type Reason = string;
export type ServerHostname = string;
export type PongMessage = [
  "PONG",
  Authenticated,
  Reason,
  ServerHostname,
  SharedKeyHexDigest
];
export type Chunk = string;

export type AckMessage = {
  ack: Chunk;
};

export type Option = {
  size?: number;
  chunk?: Chunk;
  compressed?: "gzip";
};

export type CompressedOption = Option & {
  compressed: "gzip";
};

export type Tag = string;
export type Time = number | EventTime;
export type EventRecord = Record<string, any>;
export type MessagePackEventStream = Uint8Array;

export type Entry = [Time, EventRecord];

export type MessageMode = [Tag, Time, EventRecord, Option?];
export type ForwardMode = [Tag, Entry[], Option?];
export type PackedForwardMode = [Tag, MessagePackEventStream, Option?];
export type CompressedPackedForwardMode = [
  Tag,
  MessagePackEventStream,
  CompressedOption
];

export type ServerHandshakeMessage = HeloMessage | PongMessage;
export type ClientHandshakeMessage = PingMessage;

export type ServerTransportMessage = AckMessage;
export type ClientTransportMessage =
  | MessageMode
  | ForwardMode
  | PackedForwardMode
  | CompressedPackedForwardMode;

export type ServerMessage = ServerTransportMessage | ServerHandshakeMessage;
export type ClientMessage = ClientTransportMessage | ClientHandshakeMessage;

// First part of the handshake
export type ServerKeyInfo = {
  key: SharedKey;
  nonce: SharedKeyNonce;
};

export type SharedKeyInfo = ServerKeyInfo & {
  salt: SharedKeySalt;
};

export type ClientAuthInfo = {
  salt: AuthSalt;
  username: Username;
  password: Password;
};

export type ServerAuthInfo = {
  salt: AuthSalt;
  userDict: Record<Username, Password>;
};

type PingResult = {
  sharedKeyInfo: SharedKeyInfo;
};

// Type checking functions
// These do a lot of type validation, which is bad for performance, but avoids multiple msgpack security vulnerabilities
// The performance hit shouldn't be too bad on the client, as the only large messages the client receives are in the handshake phase
// The performance hit is worse on the server, but protects the server from DOS vulnerabilities

export const isHelo = (message: any): message is HeloMessage => {
  if (
    !Array.isArray(message) ||
    message.length !== 2 ||
    message[0] !== "HELO"
  ) {
    return false;
  }
  if (typeof message[1] !== "object" || message[1] === null) {
    return false;
  }
  if (
    typeof message[1].nonce !== "string" &&
    !(
      typeof message[1].nonce === "object" &&
      message[1].nonce instanceof Uint8Array
    )
  ) {
    return false;
  }
  if (
    typeof message[1].auth !== "undefined" &&
    typeof message[1].auth !== "string" &&
    !(
      typeof message[1].auth === "object" &&
      message[1].auth instanceof Uint8Array
    )
  ) {
    return false;
  }
  if (
    typeof message[1].keepalive !== "undefined" &&
    typeof message[1].keepalive !== "boolean"
  ) {
    return false;
  }
  return true;
};

export const isPing = (message: any): message is PingMessage => {
  if (
    !Array.isArray(message) ||
    message.length !== 6 ||
    message[0] !== "PING"
  ) {
    return false;
  }
  return message.every((v, i) =>
    i === 2
      ? typeof v === "string" || v instanceof Uint8Array
      : typeof v === "string"
  );
};

export const isPong = (message: any): message is PongMessage => {
  if (
    !Array.isArray(message) ||
    message.length !== 5 ||
    message[0] !== "PONG"
  ) {
    return false;
  }
  return message.every((v, i) =>
    i === 1 ? typeof v === "boolean" : typeof v === "string"
  );
};

export const isAck = (message: any): message is AckMessage => {
  return (
    !!message &&
    message.hasOwnProperty("ack") &&
    typeof message.ack === "string"
  );
};

export const isTime = (data: any): data is Time => {
  return typeof data === "number" || data instanceof EventTime;
};

export const isOption = (data: any): data is Option | undefined => {
  if (typeof data === "undefined" || data === null) {
    return true;
  }
  if (typeof data !== "object") {
    return false;
  }
  if (typeof data.size !== "undefined" && typeof data.size !== "number") {
    return false;
  }
  if (typeof data.chunk !== "undefined" && typeof data.chunk !== "string") {
    return false;
  }
  if (
    typeof data.compressed !== "undefined" &&
    typeof data.compressed !== "string"
  ) {
    return false;
  }
  return true;
};

export const isTag = (data: any): data is Tag => {
  return typeof data === "string";
};

export const isEventRecord = (data: any): data is EventRecord => {
  return typeof data === "object" && data !== null;
};

export const isEntry = (data: any): data is Entry => {
  return (
    Array.isArray(data) &&
    data.length === 2 &&
    isTime(data[0]) &&
    isEventRecord(data[1])
  );
};

export const isMessageMode = (message: any): message is MessageMode => {
  if (
    !Array.isArray(message) ||
    (message.length !== 4 && message.length !== 3)
  ) {
    return false;
  }
  return (
    isTag(message[0]) &&
    isTime(message[1]) &&
    isEventRecord(message[2]) &&
    isOption(message[3])
  );
};

export const isForwardMode = (message: any): message is ForwardMode => {
  if (
    !Array.isArray(message) ||
    (message.length !== 3 && message.length !== 2)
  ) {
    return false;
  }
  return (
    isTag(message[0]) &&
    Array.isArray(message[1]) &&
    message[1].every(isEntry) &&
    isOption(message[2])
  );
};

export const isPackedForwardMode = (
  message: any
): message is PackedForwardMode => {
  if (
    !Array.isArray(message) ||
    (message.length !== 3 && message.length !== 2)
  ) {
    return false;
  }
  return (
    isTag(message[0]) &&
    message[1] instanceof Uint8Array &&
    isOption(message[2]) &&
    message[2]?.compressed !== "gzip"
  );
};

export const isCompressedPackedForwardMode = (
  message: any
): message is CompressedPackedForwardMode => {
  if (!Array.isArray(message) || message.length !== 3) {
    return false;
  }
  return (
    isTag(message[0]) &&
    message[1] instanceof Uint8Array &&
    isOption(message[2]) &&
    message[2]?.compressed === "gzip"
  );
};

export const isServerHandshakeMessage = (
  message: any
): message is ServerHandshakeMessage => {
  return isHelo(message) || isPong(message);
};

export const isClientHandshakeMessage = (
  message: any
): message is ClientHandshakeMessage => {
  return isPing(message);
};

export const isServerTransportMessage = (
  message: any
): message is ServerTransportMessage => {
  return isAck(message);
};

export const isClientTransportMessage = (
  message: any
): message is ClientTransportMessage => {
  return (
    isMessageMode(message) ||
    isForwardMode(message) ||
    isPackedForwardMode(message) ||
    isCompressedPackedForwardMode(message)
  );
};

export const isServerMessage = (message: any): message is ServerMessage => {
  return isServerHandshakeMessage(message) || isServerTransportMessage(message);
};

export const isClientMessage = (message: any): message is ClientMessage => {
  return isClientHandshakeMessage(message) || isClientTransportMessage(message);
};

const sharedKeyHash = (
  hostname: ClientHostname | ServerHostname,
  sharedKeyInfo: SharedKeyInfo
): SharedKeyHexDigest => {
  return crypto
    .createHash("sha512")
    .update(sharedKeyInfo.salt)
    .update(hostname)
    .update(sharedKeyInfo.nonce)
    .update(sharedKeyInfo.key)
    .digest("hex");
};

const userPasswordHash = (authInfo: ClientAuthInfo): PasswordDigest => {
  const passwordHexDigest = crypto
    .createHash("sha512")
    .update(authInfo.salt)
    .update(authInfo.username)
    .update(authInfo.password)
    .digest("hex");
  return passwordHexDigest;
};

export const generateHelo = (
  nonce: SharedKeyNonce,
  auth: AuthSalt,
  keepalive: Keepalive
): HeloMessage => {
  // ['HELO', options(hash)]
  return [
    "HELO",
    {
      nonce,
      auth,
      keepalive,
    },
  ];
};

export const parseHelo = (m: HeloMessage): HeloOptions => {
  return m[1];
};

export const generatePing = (
  hostname: ClientHostname,
  sharedKeyInfo: SharedKeyInfo,
  authInfo?: ClientAuthInfo
): PingMessage => {
  const sharedKeyHexDigest = sharedKeyHash(hostname, sharedKeyInfo);

  let userName = "";
  let passwordHexDigest = "";
  if (authInfo) {
    userName = authInfo.username;
    passwordHexDigest = userPasswordHash(authInfo);
  }
  return [
    "PING",
    hostname,
    sharedKeyInfo.salt,
    sharedKeyHexDigest,
    userName,
    passwordHexDigest,
  ];
};

/**
 * Validates a PING message from the client
 *
 * Assumes a valid structure (isPing has been called)
 *
 * @param m The ping message to validate
 * @param serverHostname The hostname of the client
 * @param serverKeyInfo The key info known to the server
 * @param authInfo Authentication information to validate (optional, auth not required if missing)
 * @returns An object with the complete SharedKeyInfo
 * @throws Error on mismatches
 */
export const checkPing = (
  m: PingMessage,
  serverHostname: ServerHostname,
  serverKeyInfo: ServerKeyInfo,
  authInfo?: ServerAuthInfo
): PingResult => {
  // ['PING', self_hostname, shared_key_salt, sha512_hex(shared_key_salt + self_hostname + nonce + shared_key), username || '', sha512_hex(auth_salt + username + password) || '']
  const clientHostname = m[1];
  const sharedKeySalt = m[2];
  const sharedKeyHexDigest = m[3];
  const username = m[4];
  const passwordDigest = m[5];
  const serverSideDigest = sharedKeyHash(clientHostname, {
    salt: sharedKeySalt,
    ...serverKeyInfo,
  });
  if (clientHostname === serverHostname) {
    throw new ConfigError(
      "Same hostname between input and output: invalid configuration"
    );
  }
  if (sharedKeyHexDigest !== serverSideDigest) {
    throw new SharedKeyMismatchError("shared key mismatch");
  }
  if (authInfo) {
    if (!username) {
      throw new AuthError("missing authentication information");
    }
    if (!authInfo.userDict.hasOwnProperty(username)) {
      throw new AuthError("username/password mismatch");
    }
    const serverSidePasswordDigest = userPasswordHash({
      salt: authInfo.salt,
      username,
      password: authInfo.userDict[username],
    });
    if (passwordDigest !== serverSidePasswordDigest) {
      throw new AuthError("username/password mismatch");
    }
  }
  return {sharedKeyInfo: {...serverKeyInfo, salt: sharedKeySalt}};
};

export const generatePong = (
  hostname: ServerHostname,
  authenticated: Authenticated,
  reason: Reason,
  sharedKeyInfo?: SharedKeyInfo
): PongMessage => {
  let sharedKeyHexDigest = "";
  if (authenticated && sharedKeyInfo) {
    sharedKeyHexDigest = sharedKeyHash(hostname, sharedKeyInfo);
  }
  return ["PONG", authenticated, reason, hostname, sharedKeyHexDigest];
};

/**
 * Checks the PONG message from the server
 *
 * Assumes a valid structure (isPong has been called)
 * @param m The PONG message from the server to validate
 * @param clientHostname The client hostname
 * @param sharedKeyInfo The client shared key information
 * @throws Error on validation issues
 */
export const checkPong = (
  m: PongMessage,
  clientHostname: ClientHostname,
  sharedKeyInfo: SharedKeyInfo
): void => {
  // [
  //   'PONG',
  //   bool(authentication result),
  //   'reason if authentication failed',
  //   server_hostname,
  //   sha512_hex(salt + server_hostname + nonce + sharedkey)
  // ]
  const authResult = m[1];
  const reason = m[2];
  const serverHostname = m[3];
  const sharedKeyHexdigest = m[4];
  if (serverHostname === clientHostname) {
    throw new ConfigError(
      "Same hostname between input and output: invalid configuration"
    );
  }
  if (!authResult) {
    throw new AuthError(`Authentication failed: ${reason}`);
  }
  const clientSideHexDigest = sharedKeyHash(serverHostname, sharedKeyInfo);
  if (sharedKeyHexdigest !== clientSideHexDigest) {
    throw new SharedKeyMismatchError("shared key mismatch");
  }
};

export const generateAck = (ack: string): AckMessage => {
  return {
    ack,
  };
};

const maybeChunk = (chunk?: Chunk): Option => {
  if (chunk) {
    return {chunk};
  } else {
    return {};
  }
};

export const generateMessageMode = (
  tag: Tag,
  time: Time,
  event: EventRecord,
  chunk?: Chunk
): MessageMode => {
  return [tag, time, event, maybeChunk(chunk)];
};

export const generateForwardMode = (
  tag: Tag,
  entries: Entry[],
  chunk?: Chunk
): ForwardMode => {
  return [tag, entries, {size: entries.length, ...maybeChunk(chunk)}];
};

export const generatePackedForwardMode = (
  tag: Tag,
  packedEntries: Uint8Array[],
  packedEntryLength: number,
  chunk?: Chunk
): PackedForwardMode => {
  const combinedEntries = Buffer.concat(packedEntries, packedEntryLength);
  const option: Option = {
    size: packedEntries.length,
    ...maybeChunk(chunk),
  };
  return [tag, combinedEntries, option];
};

export const generateCompressedPackedForwardMode = (
  tag: Tag,
  packedEntries: Uint8Array[],
  packedEntryLength: number,
  chunk?: Chunk
): CompressedPackedForwardMode => {
  const combinedEntries = zlib.gzipSync(
    Buffer.concat(packedEntries, packedEntryLength)
  );
  const option: CompressedOption = {
    size: packedEntries.length,
    ...maybeChunk(chunk),
    compressed: "gzip",
  };
  return [tag, combinedEntries, option];
};

export const generateEntry = (time: Time, event: EventRecord): Entry => {
  return [time, event];
};

export type DecodedEntries = {
  tag: Tag;
  entries: Entry[];
  /**
   * The chunk from the transport message, if any, used for acks
   */
  chunk?: Chunk;
};

/**
 * Parses a transport message from the client
 *
 * @param message The transport message to parse
 * @returns An object with the decoded entries from the object
 */
export const parseTransport = (
  message: ClientTransportMessage
): DecodedEntries => {
  if (isMessageMode(message)) {
    const options = message[3];
    const tag = message[0];
    const entry: Entry = [message[1], message[2]];
    return {
      tag,
      entries: [entry],
      chunk: options?.chunk,
    };
  } else if (isForwardMode(message)) {
    const tag = message[0];
    const entries = message[1];
    const options = message[2];
    return {
      tag,
      entries,
      chunk: options?.chunk,
    };
  } else if (
    isPackedForwardMode(message) ||
    isCompressedPackedForwardMode(message)
  ) {
    const tag = message[0];
    let entryBuffer = message[1];
    const options = message[2];
    if (isCompressedPackedForwardMode(message)) {
      entryBuffer = zlib.gunzipSync(entryBuffer);
    }
    const entries = decodeEntries(entryBuffer);

    return {
      tag,
      entries,
      chunk: options?.chunk,
    };
  } else {
    throw new DecodeError("Expected transport message, but got something else");
  }
};

/**
 * The [EventTime](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format) ext code is 0
 */
const EVENT_TIME_EXT_TYPE = 0x00;
const extensionCodec = new ExtensionCodec();
extensionCodec.register({
  type: EVENT_TIME_EXT_TYPE,
  encode: (object: unknown): Uint8Array | null => {
    if (object instanceof EventTime) {
      return EventTime.pack(object);
    } else {
      return null;
    }
  },
  decode: (data: Uint8Array) => {
    let buffer;
    if (!Buffer.isBuffer(data)) {
      buffer = Buffer.from(data);
    } else {
      buffer = data;
    }
    return EventTime.unpack(buffer);
  },
});

/**
 * Creates a newEncoder
 *
 * We can't share these because we were running into strange bugs where the internal buffers were being overwritten
 * @returns A new Encoder to use
 */
const encoder = (): Encoder => new Encoder(extensionCodec);
/**
 * Creates a new Decoder
 *
 * We can't share these because we were running into strange bugs where the internal buffers were being overwritten
 * @returns A new Decoder to use
 */
const decoder = (): Decoder => new Decoder(extensionCodec);

export const encode = (item: any): Uint8Array => {
  return encoder().encode(item);
};

export const decode = (data: Uint8Array): unknown => {
  return decoder().decode(data);
};

export const packEntry = (entry: Entry): Uint8Array => {
  return encode(entry);
};

export const decodeServerMessage = (data: Uint8Array): ServerMessage => {
  return decode(data) as ServerMessage;
};

export const decodeClientMessage = (data: Uint8Array): ClientMessage => {
  return decode(data) as ClientMessage;
};

export const encodeMessage = (
  item: ServerMessage | ClientMessage
): Uint8Array => {
  return encode(item);
};

/**
 * Decodes a stream of data from the client
 *
 * @param dataStream A Readable to read the data from
 * @returns An iterable of messages from the client, not type checked
 */
export const decodeClientStream = (
  dataStream: Readable
): AsyncIterable<ClientMessage> => {
  // This is a hack to avoid messagepack utf8-ization of the msgpack str format, since it mangles data.
  // Fluentd, when using the out_forward plugin, will pass the data in PackedForward mode using a str to represent the packed Forward messages.
  // This would normally end up getting decoded as utf8 by @msgpack/msgpack, and turned into complete garbage. This short circuits that function in the parser,
  const streamDecoder = decoder() as any;
  streamDecoder._decodeUtf8String = streamDecoder.decodeUtf8String;
  streamDecoder.decodeUtf8String = function (
    this: typeof streamDecoder,
    byteLength: number,
    headerOffset: number
  ) {
    if (this.bytes.byteLength < this.pos + headerOffset + byteLength) {
      // Defer to the error handling inside the normal function, if we don't have enough data to parse
      return this._decodeUtf8String(byteLength, headerOffset);
    }
    const offset = this.pos + headerOffset;
    // If the first byte is 0x92 (fixarr of size 2), this represents a msgpack str encoded entry
    // Also catch 0xdc and 0xdd, which represents arrays. This should never be passed, fixarr is more efficient, but just to cover all bases.
    // If the first byte is 0x1f, then assume it is compressed
    if (
      this.bytes[offset] === 0x92 ||
      this.bytes[offset] === 0x1f ||
      this.bytes[offset] === 0xdc ||
      this.bytes[offset] === 0xdd
    ) {
      return this.decodeBinary(byteLength, headerOffset);
    } else {
      return this._decodeUtf8String(byteLength, headerOffset);
    }
  }.bind(streamDecoder);
  return streamDecoder.decodeStream(dataStream) as AsyncIterable<ClientMessage>;
};

/**
 * Decodes a stream of data from the server
 *
 * @param dataStream A Readable to read the data from
 * @returns An iterable of messages from the server, not type checked
 */
export const decodeServerStream = (
  dataStream: Readable
): AsyncIterable<ServerMessage> => {
  return decoder().decodeStream(dataStream) as AsyncIterable<ServerMessage>;
};

/**
 * Decodes a sequence of entries from a Buffer
 *
 * Useful for PackedForward|CompressedPackedForward event modes
 * @param data The data to unpack
 * @returns The entries from the data
 */
export const decodeEntries = (data: Uint8Array): Entry[] => {
  const entries = Array.from(decoder().decodeMulti(data));
  if (entries.every(isEntry)) {
    return entries as Entry[];
  } else {
    throw new DecodeError("Received invalid entries");
  }
};
