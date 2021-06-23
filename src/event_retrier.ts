import {DroppedError} from "./error";

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
        // Ignore DroppedError by default, this prevents us from requeuing on shutdown
        if (e instanceof DroppedError) {
          throw e;
        }
        if (retryAttempts >= this.options.attempts) {
          throw e;
        }

        this.options.onError(e);

        const retryInterval = Math.min(
          this.options.maxDelay,
          Math.max(
            this.options.minDelay,
            this.options.backoff ** retryAttempts * this.options.delay
          )
        );
        retryAttempts += 1;
        await new Promise(r => setTimeout(r, retryInterval));
      }
      // eslint-disable-next-line no-constant-condition
    } while (true);
  }
}
