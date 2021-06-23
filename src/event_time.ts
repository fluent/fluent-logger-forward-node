/**
 * TS/JS representation of the [Fluentd EventTime](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format) type
 */
class EventTime {
  /**
   * The epoch of this EventTime (seconds since midnight, Jan 1st, 1970)
   */
  public get epoch(): number {
    return this._epoch;
  }
  /**
   * The nano part of this EventTime (epoch + nano = timestamp nanos)
   */
  public get nano(): number {
    return this._nano;
  }

  private _epoch: number;
  private _nano: number;

  /**
   * Creates a new EventTime object
   * @param epoch The epoch (seconds since midnight, Jan 1st, 1970)
   * @param nano The nano part (epoch + nano = timestamp)
   */
  constructor(epoch: number, nano: number) {
    this._epoch = epoch;
    this._nano = nano;
  }

  /**
   * Packs the `EventTime` into a buffer
   * @internal
   * @param eventTime The `EventTime` object to pack
   * @returns The serialized `EventTime`
   */
  static pack(eventTime: EventTime): Buffer {
    const b = Buffer.allocUnsafe(8);
    b.writeUInt32BE(eventTime.epoch, 0);
    b.writeUInt32BE(eventTime.nano, 4);
    return b;
  }

  /**
   * Unpacks an `EventTime` from a buffer
   * @internal
   * @param buffer The buffer to read the `EventTime` from
   * @returns The deserialized `EventTime`.
   */
  static unpack(buffer: Buffer): EventTime {
    const e = buffer.readUInt32BE(0);
    const n = buffer.readUInt32BE(4);
    return new EventTime(e, n);
  }

  /**
   * Returns the current timestamp as an `EventTime`
   *
   * Similar to `Date.now()`
   * @returns The EventTime representation of the current timestamp
   */
  static now(): EventTime {
    const now = Date.now();
    return EventTime.fromTimestamp(now);
  }

  /**
   * Converts a `Date` to an `EventTime`.
   *
   * @param date The `Date` object to convert
   * @returns The equivalent `EventTime`.
   */
  static fromDate(date: Date): EventTime {
    const t = date.getTime();
    return EventTime.fromTimestamp(t);
  }

  /**
   * Creates a new `EventTime` from a numeric timestamp
   *
   * @param t The numeric timestamp to convert to an EventTime
   * @returns The EventTime representation of the timestamp
   */
  static fromTimestamp(t: number): EventTime {
    const epoch = Math.floor(t / 1000);
    const nano = (t % 1000) * 1000000;
    return new EventTime(epoch, nano);
  }
}

export default EventTime;
