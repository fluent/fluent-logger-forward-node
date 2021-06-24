class BaseError extends Error {
  constructor(message: string) {
    super();
    Error.captureStackTrace(this, this.constructor);
    this.name = this.constructor.name;
    this.message = message;
  }
}

/**
 * Thrown on configuration errors, e.g bad event modes
 */
export class ConfigError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when an event is emitted without either passing the tag to `client.emit` or providing a `tag_prefix`.
 */
export class MissingTagError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a client/server receives an unexpected/invalid message
 */
export class UnexpectedMessageError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a client waiting for an acknowledgement times out
 */
export class AckTimeoutError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a client tries to emit an invalid data format, e.g string as timestamp, string event, etc.
 */
export class DataTypeError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a socket times out, but in a weird state.
 */
export class SocketTimeoutError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a client tries to write to a socket, but the socket is not writable
 */
export class SocketNotWritableError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when an event is dropped from the queue for any reason
 */
export class DroppedError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when an event is dropped as a result of clearing out the queue on shutdown
 *
 * Extends DroppedError since the message was dropped from the queue
 */
export class QueueShutdownError extends DroppedError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown into the ack queue on shutdown, terminating all promises waiting for an ack
 */
export class AckShutdownError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when the EventRetrier is shut down
 */
export class RetryShutdownError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when authentication fails on either the client or the server
 */
export class AuthError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when the shared key doesn't match
 *
 * Extends AuthError, since the key was incorrect
 */
export class SharedKeyMismatchError extends AuthError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a message could not be decoded properly
 */
export class DecodeError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when trying to call `connect()` on a fatal socket
 */
export class FatalSocketError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}
