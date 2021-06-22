class BaseError extends Error {
  constructor(message: string) {
    super();
    Error.captureStackTrace(this, this.constructor);
    this.name = this.constructor.name;
    this.message = message;
  }
}

export class ConfigError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class MissingTagError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class ResponseError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class AckTimeoutError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class DataTypeError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class SocketTimeoutError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class SocketNotWritableError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class DroppedError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class AuthError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class ShutdownError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class SharedKeyMismatchError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class DecodeError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class FatalSocketError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}
