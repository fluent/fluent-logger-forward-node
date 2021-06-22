import * as EventEmitter from "events";
import {Duplex, PassThrough, Readable, Writable} from "stream";
import * as duplexer3 from "duplexer3";

export const fakeSocket = (): {
  socket: Duplex;
  readable: Readable;
  writable: Writable;
} => {
  const readable = new PassThrough();
  const writable = new PassThrough();
  return {
    socket: duplexer3(readable, writable),
    readable,
    writable,
  };
};

export class TestSocket extends EventEmitter {
  public connected = false;
  public isWritable = false;
  public onWrite: (data: Uint8Array) => Promise<void> = () => Promise.resolve();

  public connect() {
    this.connected = true;
    this.isWritable = true;
    this.emit("writable");
  }

  public disconnect() {
    this.isWritable = false;
    this.connected = false;
  }

  public writable() {
    return this.isWritable;
  }

  public write(data: Uint8Array): Promise<void> {
    return this.onWrite(data);
  }
}
