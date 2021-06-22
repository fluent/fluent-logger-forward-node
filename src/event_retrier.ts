import {DroppedError} from "./error";

export type EventRetryOptions = {
  attempts: number;
  backoff: number;
  delay: number;
  minDelay: number;
  maxDelay: number;
  onError: (err: Error) => void;
};

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
