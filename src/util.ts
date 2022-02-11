export const awaitAtMost = <T>(
  promise: Promise<T>,
  timeout: number
): Promise<unknown> => {
  let timeoutId: NodeJS.Timeout | null = null;
  const racePromise = Promise.race<Promise<unknown>>([
    promise,
    new Promise<void>(
      resolve =>
        (timeoutId = setTimeout(() => {
          timeoutId = null;
          resolve();
        }, timeout))
    ),
  ]);
  racePromise
    .finally(() => {
      if (timeoutId) clearTimeout(timeoutId);
    })
    .catch(() => {});
  return racePromise;
};

export const awaitNextTick = (): Promise<void> => {
  return new Promise(r => process.nextTick(r));
};

export const awaitTimeout = (timeout: number): Promise<void> => {
  return new Promise(r => setTimeout(r, timeout));
};

export interface DeferredPromise<T> {
  promise: Promise<T>;
  resolve(value?: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

export const pDefer = <T>(): DeferredPromise<T> => {
  const deferred: Partial<DeferredPromise<T>> = {};
  deferred.promise = new Promise((resolve, reject) => {
    deferred.resolve = resolve;
    deferred.reject = reject;
  });
  return deferred as DeferredPromise<T>;
};
