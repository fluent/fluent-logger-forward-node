import * as pDefer from "p-defer";
import {Queue, PacketData} from "./queue";
import * as protocol from "../protocol";

type EventRecord = {
  tag: protocol.Tag;
  time: protocol.Time;
  event: protocol.EventRecord;
  deferred: pDefer.DeferredPromise<void>;
};

export class MessageQueue extends Queue {
  private sendQueue: Set<EventRecord> = new Set();

  // Size is not measured for this queue
  get queueSize(): number {
    return -1;
  }

  get queueLength(): number {
    return this.sendQueue.size;
  }

  public push(
    tag: string,
    time: protocol.Time,
    event: protocol.EventRecord
  ): Promise<void> {
    const deferred = pDefer<void>();
    this.sendQueue.add({
      tag,
      time,
      event,
      deferred,
    });
    return deferred.promise;
  }

  public pop(): EventRecord | null {
    if (this.sendQueue.size === 0) {
      return null;
    }
    const entryData = this.sendQueue.values().next().value;
    this.sendQueue.delete(entryData);
    return entryData;
  }

  public nextPacket(chunk?: protocol.Chunk): PacketData | null {
    const entryData = this.pop();
    if (entryData === null) {
      return null;
    }

    const packet = protocol.generateMessageMode(
      entryData.tag,
      entryData.time,
      entryData.event,
      chunk
    );
    return {
      packet: protocol.encodeMessage(packet),
      deferred: entryData.deferred,
    };
  }
}
