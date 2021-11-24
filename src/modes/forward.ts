import {Queue, PacketData} from "./queue";
import * as protocol from "../protocol";
import {pDefer, DeferredPromise} from "../util";

type ForwardRecord = {
  tag: protocol.Tag;
  entries: protocol.Entry[];
  deferred: DeferredPromise<void>;
};

/**
 * Implements the Forward specification's [Forward mode](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#forward-mode)
 */
export class ForwardQueue extends Queue {
  /**
   * Maintain the queue as a Map
   *
   * JS guarantees maps are insertion ordered, so calling sendQueue.values().next.value will be the first tag to be inserted.
   */
  private sendQueue: Map<protocol.Tag, ForwardRecord> = new Map();
  /**
   * The total number of events stored within the queue
   *
   * Note that this isn't just sendQueue.size because each entry in the map can have multiple events
   */
  private sendQueueLength = 0;

  /**
   * Size is not measured for this queue
   */
  get queueSize(): number {
    return -1;
  }

  get queueLength(): number {
    return this.sendQueueLength;
  }

  public push(
    tag: protocol.Tag,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void> {
    const entry = protocol.generateEntry(time, data);
    this.sendQueueLength += 1;
    if (this.sendQueue.has(tag)) {
      const entryData = this.sendQueue.get(tag) as ForwardRecord;
      entryData.entries.push(entry);
      return entryData.deferred.promise;
    } else {
      const deferred = pDefer<void>();
      this.sendQueue.set(tag, {
        tag,
        entries: [entry],
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
