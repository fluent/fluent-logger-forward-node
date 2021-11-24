import {DroppedError, RetryShutdownError} from "./error";
import {awaitAtMost, pDefer, DeferredPromise} from "./util";

/**
 * Event retry settings
 *
 * The parameters represent an exponential backoff formula:
 * min(maxDelay, max(minDelay, backoff^attempts * delay))
 */
export type EventRetryOptions = {
  /**
   * How often we retry each event
   *
   * Defaults to 4
   */
  attempts: number;
  /**
   * The backoff factor for each attempt
   *
   * Defaults to 2
   */
  backoff: number;
  /**
   * The delay factor for each attempt
   *
   * Defaults to 100
   */
  delay: number;
  /**
   * The global minimum delay
   */
  minDelay: number;
  /**
   * The global maximum delay
   */
  maxDelay: number;
  /**
   * Called with each error
   *
   * Can be used for logging, or if the error is non-retryable, this callback can `throw` the error to short circuit the callback.
   */
  onError: (err: Error) => void;
};

/**
 * Provides retry logic for a promise, with failure cases
 */
export class EventRetrier {
  private options: EventRetryOptions;
  private cancelWait: DeferredPromise<void>;
  constructor(opts: Partial<EventRetryOptions> = {}) {
    this.options = {
      attempts: 4,
      backoff: 2,
      delay: 100,
      minDelay: -Infinity,
      maxDelay: +Infinity,
      onError: () => {},
      ...(opts || {}),
    };
    this.cancelWait = pDefer<void>();
  }

  /**
   * Causes ongoing retry handlers to cancel their timeout and immediately retry
   * @returns a Promise which completes after all handlers have retried once.
   */
  public async shortCircuit(): Promise<void> {
    const {resolve} = this.cancelWait;
    // Reinitialize the promise so this class can continue to be reused
    this.cancelWait = pDefer<void>();

    resolve();

    // The Promise.resolve PromiseJob will be enqueued after all the resolve functions on cancelWait.
    // Promise.race([a,b]) then enqueues another PromiseJob since it needs to resolve its promise.
    // We want the shutdown promise to resolve after the Promise.race.then call, meaning we enqueue a
    // new PromiseJob as well, which is enqueued after Promise.race enqueues it's jobs
    await Promise.resolve();
  }

  /**
   * Exits all ongoing retry handlers with an error
   *
   * @returns A promise which completes once all retry handlers have exited.
   */
  public async shutdown(): Promise<void> {
    const {reject} = this.cancelWait;
    // Reinitialize the promise so this class can continue to be reused
    this.cancelWait = pDefer<void>();

    reject(new RetryShutdownError("Retries were shut down"));

    // The Promise.resolve PromiseJob will be enqueued after all the resolve functions on cancelWait.
    // Promise.race([a,b]) then enqueues another PromiseJob since it needs to resolve its promise.
    // We want the shutdown promise to resolve after the Promise.race.then call, meaning we enqueue a
    // new PromiseJob as well, which is enqueued after Promise.race enqueues it's jobs
    await Promise.resolve();
  }

  /**
   * Retry the promise
   *
   * Attempts the promise in an infinite loop, and retries according to the logic in EventRetryOptions
   * @param makePromise An async function to retry
   * @returns A Promise which succeeds if the async function succeeds, or has exhausted retry attempts
   */
  public async retryPromise<T>(makePromise: () => Promise<T>): Promise<T> {
    let retryAttempts = 0;
    do {
      try {
        return await makePromise();
      } catch (e) {
        // Ignore DroppedError by default, this prevents us from requeuing on shutdown or queue clear
        if (e instanceof DroppedError) {
          throw e;
        }
        if (retryAttempts >= this.options.attempts) {
          throw e;
        }

        this.options.onError(e as Error);

        const retryInterval = Math.min(
          this.options.maxDelay,
          Math.max(
            this.options.minDelay,
            this.options.backoff ** retryAttempts * this.options.delay
          )
        );
        retryAttempts += 1;

        // Await the retry promise, but short circuiting is OK
        await awaitAtMost(this.cancelWait.promise, retryInterval);
      }
      // eslint-disable-next-line no-constant-condition
    } while (true);
  }
}
