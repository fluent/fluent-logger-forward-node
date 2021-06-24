import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";
import * as pDefer from "p-defer";
import {EventRetrier} from "../src/event_retrier";
import {RetryShutdownError} from "../src/error";

chai.use(chaiAsPromised);
const expect = chai.expect;

const sandbox = sinon.createSandbox();

describe("EventRetrier", () => {
  afterEach(() => {
    sandbox.restore();
  });
  it("should retry events", async () => {
    const makePromise = sandbox.stub();
    makePromise.onFirstCall().rejects();
    makePromise.onSecondCall().resolves();

    const retrier = new EventRetrier();

    await retrier.retryPromise(makePromise);

    sinon.assert.calledTwice(makePromise);
  });

  it("should successfully short circuit", async () => {
    const clock = sandbox.useFakeTimers();
    const clearTimeoutSpy = sandbox.spy(clock, "clearTimeout");

    const firstCall = pDefer<void>();
    const makePromise = sandbox.stub();
    makePromise.onFirstCall().callsFake(() => {
      firstCall.resolve();
      return Promise.reject();
    });
    makePromise.onSecondCall().resolves();

    // long delay
    const retrier = new EventRetrier({minDelay: 50000000});

    const retryPromise = retrier.retryPromise(makePromise);

    sinon.assert.calledOnce(makePromise);

    await expect(firstCall.promise).to.eventually.be.fulfilled;

    expect(retryPromise).to.not.be.fulfilled;

    await retrier.shortCircuit();

    sinon.assert.calledTwice(makePromise);

    expect(retryPromise).to.be.fulfilled;

    // Make sure we cleared out the timeout after short circuit
    sinon.assert.calledOnce(clearTimeoutSpy);
  });

  it("should successfully shut down", async () => {
    const clock = sandbox.useFakeTimers();
    const clearTimeoutSpy = sandbox.spy(clock, "clearTimeout");

    const firstCall = pDefer<void>();
    const makePromise = sandbox.stub();
    makePromise.onFirstCall().callsFake(() => {
      firstCall.resolve();
      return Promise.reject();
    });
    makePromise.onSecondCall().resolves();

    // long delay
    const retrier = new EventRetrier({minDelay: 50000000});

    const retryPromise = retrier.retryPromise(makePromise);

    sinon.assert.calledOnce(makePromise);

    await expect(firstCall.promise).to.eventually.be.fulfilled;

    expect(retryPromise).to.not.be.fulfilled;

    await retrier.shutdown();

    sinon.assert.calledOnce(makePromise);

    expect(retryPromise).to.be.rejectedWith(RetryShutdownError);

    // Make sure we cleared out the timeout after shutdown
    sinon.assert.calledOnce(clearTimeoutSpy);
  });
});
