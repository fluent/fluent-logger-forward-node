import {PacketData, Queue} from "./queue";
import * as protocol from "../protocol";
import {pDefer, DeferredPromise} from "../util";

type PackedRecord = {
  tag: protocol.Tag;
  entries: Uint8Array[];
  /**
   * The total length of the buffers in entries
   *
   * Useful for concatenating them all together later
   */
  size: number;
  deferred: DeferredPromise<void>;
};

/**
 * Implements the Forward specification's [(Compressed)?PackedForward mode](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#packedforward-mode)
 *
 * Implements both in the same queue, because the only code difference is compression in `nextPacket`.
 *
 * Subclassed below into the correct modes.
 */
class BasePackedForwardQueue extends Queue {
  /**
   * Maintain the queue as a Map
   *
   * JS guarantees maps are insertion ordered, so calling sendQueue.values().next.value will be the first tag to be inserted.
   */
  private sendQueue: Map<protocol.Tag, PackedRecord> = new Map();
  /**
   * The total size of the buffers in the queue
   */
  private sendQueueSize = 0;
  /**
   * The total number of events stored within the queue
   *
   * Note that this isn't just sendQueue.size because each entry in the map can have multiple events
   */
  private sendQueueLength = 0;
  /**
   * Used to gate compression of `nextPacket`. The difference between PackedForward and CompressedPackedForward
   */
  protected compressed = false;

  get queueSize(): number {
    return this.sendQueueSize;
  }

  get queueLength(): number {
    return this.sendQueueLength;
  }

  public push(
    tag: protocol.Tag,
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

  protected pop(): PackedRecord | null {
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

/**
 * Implements the Forward specification's [PackedForward mode](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#packedforward-mode)
 */
export class PackedForwardQueue extends BasePackedForwardQueue {
  protected compressed = false;
}

/**
 * Implements the Forward specification's [CompressedPackedForward mode](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#compressedpackedforward-mode)
 */
export class CompressedPackedForwardQueue extends BasePackedForwardQueue {
  protected compressed = true;
}
