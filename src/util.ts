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
  racePromise.finally(() => (timeoutId ? clearTimeout(timeoutId) : undefined));
  return racePromise;
};

export const awaitNextTick = (): Promise<void> => {
  return new Promise(r => process.nextTick(r));
};

export const awaitTimeout = (timeout: number): Promise<void> => {
  return new Promise(r => setTimeout(r, timeout));
};
