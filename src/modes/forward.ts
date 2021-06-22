import * as pDefer from "p-defer";
import {Queue, PacketData} from "./queue";
import * as protocol from "../protocol";

type ForwardRecord = {
  tag: protocol.Tag;
  entries: protocol.Entry[];
  size: number;
  deferred: pDefer.DeferredPromise<void>;
};

export class ForwardQueue extends Queue {
  private sendQueue: Map<protocol.Tag, ForwardRecord> = new Map();
  private sendQueueLength = 0;

  // Size is not measured for this queue
  get queueSize(): number {
    return -1;
  }

  get queueLength(): number {
    return this.sendQueueLength;
  }

  public push(
    tag: string,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void> {
    const entry = protocol.generateEntry(time, data);
    this.sendQueueLength += 1;
    if (this.sendQueue.has(tag)) {
      const entryData = this.sendQueue.get(tag) as ForwardRecord;
      entryData.entries.push(entry);
      entryData.size += entry.length;
      return entryData.deferred.promise;
    } else {
      const deferred = pDefer<void>();
      this.sendQueue.set(tag, {
        tag,
        entries: [entry],
        size: entry.length,
        deferred: deferred,
      });
      return deferred.promise;
    }
  }

  public pop(): ForwardRecord | null {
    if (this.sendQueue.size === 0) {
      return null;
    }
    const entryData = this.sendQueue.values().next().value;
    this.sendQueue.delete(entryData.tag);
    this.sendQueueLength -= entryData.entries.length;
    return entryData;
  }

  public nextPacket(chunk?: protocol.Chunk): PacketData | null {
    const entryData = this.pop();
    if (entryData === null) {
      return null;
    }

    const packet = protocol.generateForwardMode(
      entryData.tag,
      entryData.entries,
      chunk
    );
    return {
      packet: protocol.encodeMessage(packet),
      deferred: entryData.deferred,
    };
  }
}
