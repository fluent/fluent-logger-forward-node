import * as protocol from "../protocol";
import {DroppedError, QueueShutdownError} from "../error";
import {DeferredPromise} from "../util";

/**
 * Every queue must have this type of data
 *
 * For DropEntry and clear purposes
 */
export type EntryData = {
  /**
   * A deferred promise to resolve or reject once the data is sent or dropped
   */
  deferred: DeferredPromise<void>;
};

/**
 * A packet to send to the Fluentd server
 */
export type PacketData = {
  /**
   * The data to send
   */
  packet: Uint8Array;
  /**
   * A deferred promise to resolve once the data is successfully sent
   */
  deferred: DeferredPromise<void>;
};

/**
 * Exposes a send queue for various event modes.
 */
export abstract class Queue {
  /**
   * The # of entries in the queue
   *
   * -1 if the queue implementation doesn't expose this
   */
  public abstract get queueLength(): number;
  /**
   * The total size of the queue
   *
   * -1 if the queue implementation doesn't expose this
   */
  public abstract get queueSize(): number;
  /**
   * Add an event to the queue
   *
   * @param tag
   * @param time
   * @param data
   * @returns A Promise which is resolved once the data is sent, or rejected if there is an error
   */
  public abstract push(
    tag: protocol.Tag,
    time: protocol.Time,
    data: protocol.EventRecord
  ): Promise<void>;
  /**
   * Returns the next packet to send from the queue
   *
   * @param chunk A Chunk ID to send along, for acknowledgements if enabled
   *
   */
  public abstract nextPacket(chunk?: protocol.Chunk): PacketData | null;
  /**
   * Returns and removes the first packet from the queue
   *
   * Used to drop events from the queue
   * @returns An entry, or null if the queue is empty
   */
  protected abstract pop(): EntryData | null;

  /**
   * Drops the item at the front of the queue
   *
   * Handles rejecting the promises, etc
   * @returns void
   */
  public dropEntry(): void {
    const entryData = this.pop();
    if (entryData === null) {
      return;
    } else {
      entryData.deferred.reject(
        new DroppedError("Message was dropped due to limits")
      );
    }
  }

  /**
   * Clears out the queue
   */
  public clear(): void {
    let entryData: EntryData | null;
    while ((entryData = this.pop()) !== null) {
      entryData.deferred.reject(
        new QueueShutdownError("Message dropped due to queue shutdown")
      );
    }
  }
}
