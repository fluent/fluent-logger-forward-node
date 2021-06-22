import * as protocol from "../protocol";
import {DeferredPromise} from "p-defer";
import {DroppedError} from "../error";

export type EntryData = {
  deferred: DeferredPromise<void>;
};

export type PacketData = {
  packet: Uint8Array;
  deferred: DeferredPromise<void>;
};

export abstract class Queue {
  public abstract get queueLength(): number;
  public abstract get queueSize(): number;
  public abstract push(
    tag: string,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void>;
  public abstract nextPacket(chunk?: protocol.Chunk): PacketData | null;
  public abstract pop(): EntryData | null;

  public dropEntry(): void {
    const entryData = this.pop();
    if (entryData === null) {
      return;
    } else {
      entryData.deferred.reject(
        new DroppedError("Message was dropped due to size limits")
      );
    }
  }

  public clear(): void {
    let entryData: EntryData | null;
    while ((entryData = this.pop()) !== null) {
      entryData.deferred.reject(
        new DroppedError("Message dropped due to queue shutdown")
      );
    }
  }
}
