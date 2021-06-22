import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import {FluentClient, FluentClientOptions} from "../src/client";
import * as protocol from "../src/protocol";
import EventTime from "../src/event_time";

import {TestSocket} from "./helpers";
import {
  AckTimeoutError,
  ConfigError,
  DataTypeError,
  DroppedError,
  ShutdownError,
} from "../src/error";

chai.use(chaiAsPromised);
const expect = chai.expect;

const sandbox = sinon.createSandbox();

const createFluentClient = (
  tag_prefix: string,
  options?: FluentClientOptions
) => {
  const connectStub = sandbox.stub(FluentClient.prototype, <any>"createSocket");
  const socket = new TestSocket();
  connectStub.callsFake(() => {
    return socket;
  });

  const client = new FluentClient(tag_prefix, options);

  return {client, socket, connectStub};
};

describe("FluentClient", () => {
  afterEach(() => {
    sandbox.restore();
  });

  it("should throw an error when passed an invalid event mode", () => {
    expect(() => new FluentClient("test", <any>{eventMode: "drop"})).to.throw(
      ConfigError
    );
  });

  describe("emit", () => {
    it("should submit events", async () => {
      const {client, socket} = createFluentClient("test");
      const event = {event: "foo"};
      const waitForWrite = new Promise<void>(resolve => {
        socket.onWrite = (data: Uint8Array): Promise<void> => {
          const message = protocol.parseTransport(
            protocol.decodeClientMessage(
              data
            ) as protocol.ClientTransportMessage
          );
          expect(message.tag).to.equal("test.foo");
          expect(message.entries[0][1]).to.deep.equal(event);
          resolve();
          return Promise.resolve();
        };
      });
      await client.emit("foo", event);
      await waitForWrite;
    });
    it("should submit events with different arguments", async () => {
      const {client} = createFluentClient("test");
      await client.emit({event: "foo"});
      await client.emit({event: "foo"}, Date.now());
      await client.emit("foo", {event: "foo"}, Date.now());
    });

    it("should not accept invalid data to event", async () => {
      const {client} = createFluentClient("test");
      await expect(client.emit("test", <any>"")).to.eventually.be.rejectedWith(
        DataTypeError
      );
    });

    it("should not accept invalid timestamp to event", async () => {
      const {client} = createFluentClient("test");
      await expect(
        client.emit("test", {b: "c"}, <any>"bla")
      ).to.eventually.be.rejectedWith(DataTypeError);
      await expect(
        client.emit("test", {b: "c"}, <any>new Set())
      ).to.eventually.be.rejectedWith(DataTypeError);
    });

    it("should accept custom timestamps", async () => {
      const {client} = createFluentClient("test");
      await client.emit({event: "foo"}, 1234);
      await client.emit({event: "foo"}, Date.now());
      await client.emit({event: "foo"}, EventTime.now());
    });

    it("should limit queue size", async () => {
      const {client, socket} = createFluentClient("test", {
        sendQueueMaxSize: 20,
      });
      socket.isWritable = false;
      const expectation = expect(
        client.emit("a", {event: "foo bar"})
      ).to.eventually.be.rejectedWith(DroppedError);
      const secondEvent = client.emit("b", {event: "lorem"});
      await expectation;
      socket.isWritable = true;
      socket.emit("writable");
      await secondEvent;
    });

    it("should limit queue length", async () => {
      const {client, socket} = createFluentClient("test", {
        eventMode: "Message",
        sendQueueMaxLength: 1,
      });
      socket.isWritable = false;
      const expectation = expect(
        client.emit("a", {event: "foo bar"})
      ).to.eventually.be.rejectedWith(DroppedError);
      const secondEvent = client.emit("b", {event: "lorem"});
      await expectation;
      socket.isWritable = true;
      socket.emit("writable");
      await secondEvent;
    });

    describe("when flush interval is provided", () => {
      it("should trigger flush after emit if queue is too large (size)", async () => {
        const {client} = createFluentClient("test", {
          sendQueueFlushSize: 20,
          flushInterval: 600000 /* 10 minutes */,
        });
        const firstEvent = client.emit("a", {event: "foo bar"});
        const secondEvent = client.emit("b", {event: "lorem"});

        await expect(
          Promise.race([firstEvent, new Promise((_, r) => setTimeout(r, 100))])
        ).to.eventually.be.fulfilled;
        await expect(
          Promise.race([secondEvent, new Promise((_, r) => setTimeout(r, 100))])
        ).to.eventually.be.fulfilled;
      });

      it("should trigger flush after emit if queue is too large (length)", async () => {
        const {client} = createFluentClient("test", {
          eventMode: "Message",
          sendQueueFlushLength: 2,
          flushInterval: 600000 /* 10 minutes */,
        });
        const firstEvent = client.emit("a", {event: "foo bar"});
        const secondEvent = client.emit("b", {event: "lorem"});

        await expect(
          Promise.race([firstEvent, new Promise((_, r) => setTimeout(r, 100))])
        ).to.eventually.be.fulfilled;
        await expect(
          Promise.race([secondEvent, new Promise((_, r) => setTimeout(r, 100))])
        ).to.eventually.be.fulfilled;
      });

      it("should setup flush event after emit", async () => {
        const timeout = 100;
        const {client} = createFluentClient("test", {
          flushInterval: timeout /* 100ms */,
        });

        const spy = sandbox.spy(client, <any>"innerFlush");
        const firstEvent = client.emit("a", {event: "foo bar"});
        const secondEvent = client.emit("b", {event: "lorem"});

        sinon.assert.notCalled(spy);

        await new Promise(r => setTimeout(r, timeout / 2));

        sinon.assert.notCalled(spy);

        await expect(firstEvent).to.eventually.be.fulfilled;
        await expect(secondEvent).to.eventually.be.fulfilled;

        sinon.assert.calledOnce(spy);
      });
    });
    describe("when no flush interval is provided", () => {
      it("should trigger flush after emit", async () => {
        const {client} = createFluentClient("test", {});
        const spy = sandbox.spy(client, <any>"innerFlush");
        const firstEvent = client.emit("a", {event: "foo bar"});
        const secondEvent = client.emit("b", {event: "lorem"});

        sinon.assert.notCalled(spy);

        await new Promise(r => process.nextTick(() => process.nextTick(r)));

        sinon.assert.calledOnce(spy);

        await expect(firstEvent).to.eventually.be.fulfilled;
        await expect(secondEvent).to.eventually.be.fulfilled;
      });
    });

    describe("when acks are enabled", () => {
      it("should return promise which is resolved on ack", async () => {
        const {client, socket} = createFluentClient("test", {
          ack: {},
        });
        const waitForWrite = new Promise<string | undefined>(resolve => {
          socket.onWrite = (data: Uint8Array): Promise<void> => {
            const message = protocol.parseTransport(
              protocol.decodeClientMessage(
                data
              ) as protocol.ClientTransportMessage
            );
            resolve(message.chunk);
            return Promise.resolve();
          };
        });
        const firstEvent = client.emit("a", {event: "foo bar"});

        const chunk = await waitForWrite;
        expect(chunk).not.to.be.undefined;

        if (chunk) {
          socket.emit("ack", chunk);
        }

        await expect(firstEvent).to.eventually.be.fulfilled;
      });
      it("should return promise which is rejected on ack timeout", async () => {
        const {client, socket} = createFluentClient("test", {
          ack: {ackTimeout: 50},
        });
        const waitForWrite = new Promise<string | undefined>(resolve => {
          socket.onWrite = (data: Uint8Array): Promise<void> => {
            const message = protocol.parseTransport(
              protocol.decodeClientMessage(
                data
              ) as protocol.ClientTransportMessage
            );
            resolve(message.chunk);
            return Promise.resolve();
          };
        });
        const firstEvent = client.emit("a", {event: "foo bar"});

        const chunk = await waitForWrite;
        expect(chunk).not.to.be.undefined;

        await expect(firstEvent).to.eventually.be.rejectedWith(AckTimeoutError);
      });
      it("should return promise which is rejected on write error", async () => {
        const {client, socket} = createFluentClient("test", {
          ack: {},
        });
        const waitForWrite = new Promise<string | undefined>(resolve => {
          socket.onWrite = (data: Uint8Array): Promise<void> => {
            const message = protocol.parseTransport(
              protocol.decodeClientMessage(
                data
              ) as protocol.ClientTransportMessage
            );
            resolve(message.chunk);
            return Promise.reject(new Error("test"));
          };
        });
        const firstEvent = client.emit("a", {event: "foo bar"});

        const chunk = await waitForWrite;
        expect(chunk).not.to.be.undefined;

        await expect(firstEvent).to.eventually.be.rejectedWith(Error, /test/);
      });

      it("should return promise which is rejected on disconnect", async () => {
        const {client, socket} = createFluentClient("test", {
          ack: {},
        });
        const waitForWrite = new Promise<string | undefined>(resolve => {
          socket.onWrite = (data: Uint8Array): Promise<void> => {
            const message = protocol.parseTransport(
              protocol.decodeClientMessage(
                data
              ) as protocol.ClientTransportMessage
            );
            resolve(message.chunk);
            return Promise.resolve();
          };
        });
        const firstEvent = client.emit("a", {event: "foo bar"});

        const chunk = await waitForWrite;
        expect(chunk).not.to.be.undefined;

        client.disconnect();

        await expect(firstEvent).to.eventually.be.rejectedWith(ShutdownError);
      });
    });
    it("should return promise which is rejected on write error", async () => {
      const {client, socket} = createFluentClient("test", {
        ack: {},
      });
      const waitForWrite = new Promise<void>(resolve => {
        socket.onWrite = (): Promise<void> => {
          resolve();
          return Promise.reject(new Error("test"));
        };
      });
      const firstEvent = client.emit("a", {event: "foo bar"});

      await waitForWrite;

      await expect(firstEvent).to.eventually.be.rejectedWith(Error, /test/);
    });

    it("should retry emission when it fails and retries are enabled", async () => {
      const onError = sinon.fake();
      const {client, socket} = createFluentClient("test", {
        eventRetry: {
          onError,
        },
      });
      let calls = 0;
      socket.onWrite = (): Promise<void> => {
        calls++;
        if (calls <= 1) {
          return Promise.reject(new Error("test"));
        } else {
          return Promise.resolve();
        }
      };

      const firstEvent = client.emit("a", {event: "foo bar"});

      await expect(firstEvent).to.eventually.be.fulfilled;

      sinon.assert.calledOnce(onError);
    });
  });

  it("should forward errors", done => {
    const {socket} = createFluentClient("test", {
      onSocketError: (err: Error) => {
        expect(err.message).to.equal("test");
        done();
      },
    });
    socket.emit("error", new Error("test"));
  });

  it("should reject pending events after shutdown", async () => {
    const {client, socket} = createFluentClient("test");
    socket.isWritable = false;
    const firstEvent = client.emit("a", {event: "foo bar"});

    client.shutdown();

    await expect(firstEvent).to.eventually.be.rejectedWith(DroppedError);
  });
});
