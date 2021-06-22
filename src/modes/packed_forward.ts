import * as pDefer from "p-defer";
import {PacketData, Queue} from "./queue";
import * as protocol from "../protocol";

type PackedRecord = {
  tag: protocol.Tag;
  entries: Uint8Array[];
  size: number;
  deferred: pDefer.DeferredPromise<void>;
};

class BasePackedForwardQueue extends Queue {
  private sendQueue: Map<protocol.Tag, PackedRecord> = new Map();
  private sendQueueSize = 0;
  private sendQueueLength = 0;
  protected compressed = false;

  get queueSize(): number {
    return this.sendQueueSize;
  }

  get queueLength(): number {
    return this.sendQueueLength;
  }

  public push(
    tag: string,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void> {
    const entry = protocol.packEntry(protocol.generateEntry(time, data));
    this.sendQueueSize += entry.length;
    this.sendQueueLength += 1;
    if (this.sendQueue.has(tag)) {
      const entryData = this.sendQueue.get(tag) as PackedRecord;
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

  public pop(): PackedRecord | null {
    if (this.sendQueue.size === 0) {
      return null;
    }
    const entryData = this.sendQueue.values().next().value;
    this.sendQueue.delete(entryData.tag);
    this.sendQueueLength -= entryData.entries.length;
    this.sendQueueSize -= entryData.size;
    return entryData;
  }

  public nextPacket(chunk?: protocol.Chunk): PacketData | null {
    const entryData = this.pop();
    if (entryData === null) {
      return null;
    }

    let packet;
    if (this.compressed) {
      packet = protocol.generateCompressedPackedForwardMode(
        entryData.tag,
        entryData.entries,
        entryData.size,
        chunk
      );
    } else {
      packet = protocol.generatePackedForwardMode(
        entryData.tag,
        entryData.entries,
        entryData.size,
        chunk
      );
    }
    return {
      packet: protocol.encodeMessage(packet),
      deferred: entryData.deferred,
    };
  }
}

export class PackedForwardQueue extends BasePackedForwardQueue {
  protected compressed = false;
}

export class CompressedPackedForwardQueue extends BasePackedForwardQueue {
  protected compressed = true;
}
