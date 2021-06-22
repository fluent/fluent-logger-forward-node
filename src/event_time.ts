class EventTime {
  epoch: number;
  nano: number;

  constructor(epoch: number, nano: number) {
    this.epoch = epoch;
    this.nano = nano;
  }

  static pack(eventTime: EventTime): Buffer {
    const b = Buffer.allocUnsafe(8);
    b.writeUInt32BE(eventTime.epoch, 0);
    b.writeUInt32BE(eventTime.nano, 4);
    return b;
  }

  static unpack(buffer: Buffer): EventTime {
    const e = buffer.readUInt32BE(0);
    const n = buffer.readUInt32BE(4);
    return new EventTime(e, n);
  }

  static now(): EventTime {
    const now = Date.now();
    return EventTime.fromTimestamp(now);
  }

  static fromDate(date: Date): EventTime {
    const t = date.getTime();
    return EventTime.fromTimestamp(t);
  }

  static fromTimestamp(t: number): EventTime {
    const epoch = Math.floor(t / 1000);
    const nano = (t % 1000) * 1000000;
    return new EventTime(epoch, nano);
  }
}

export default EventTime;
