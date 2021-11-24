import {Queue, PacketData} from "./queue";
import * as protocol from "../protocol";
import {pDefer, DeferredPromise} from "../util";

type EventRecord = {
  tag: protocol.Tag;
  time: protocol.Time;
  event: protocol.EventRecord;
  deferred: DeferredPromise<void>;
};

/**
 * Implements the Forward specification's [Message mode](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#message-modes)
 */
export class MessageQueue extends Queue {
  /**
   * Maintain the queue as a Set
   *
   * JS guarantees sets are insertion ordered, so calling sendQueue.values().next.value will be the first entry to be inserted.
   */
  private sendQueue: Set<EventRecord> = new Set();

  /**
   * Size is not measured for this queue
   */
  get queueSize(): number {
    return -1;
  }

  /**
   * The length of the queue
   */
  get queueLength(): number {
    return this.sendQueue.size;
  }

  public push(
    tag: protocol.Tag,
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

  protected pop(): EventRecord | null {
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
