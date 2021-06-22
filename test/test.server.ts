import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";

import * as protocol from "../src/protocol";
import {FluentServer} from "../src/server";
import {FluentClient} from "../src/client";

chai.use(chaiAsPromised);
const expect = chai.expect;

describe("FluentServer", () => {
  it("should receive messages from the client", async () => {
    const server = new FluentServer();

    await server.listen();

    const client = new FluentClient("test", {
      socket: {port: server.port || undefined, disableReconnect: true},
    });

    const eventPromise = new Promise<{
      tag: protocol.Tag;
      time: protocol.Time;
      record: protocol.EventRecord;
    }>(resolve => {
      server.once("entry", (tag, time, record) => {
        resolve({tag, time, record});
      });
    });

    const clientPromise = client.emit("hello", {data: "hello world!"});

    await clientPromise;
    const {tag, record} = await eventPromise;

    expect(tag).to.equal("test.hello");
    expect(record.data).to.equal("hello world!");

    await server.close();
  });

  it("should ack messages from the client", async () => {
    const server = new FluentServer();

    await server.listen();

    const client = new FluentClient("test", {
      socket: {port: server.port || undefined, disableReconnect: true},
      ack: {},
    });

    const eventPromise = new Promise<{
      tag: protocol.Tag;
      time: protocol.Time;
      record: protocol.EventRecord;
    }>(resolve => {
      server.once("entry", (tag, time, record) => {
        resolve({tag, time, record});
      });
    });

    const clientPromise = client.emit("hello", {data: "hello world!"});

    const {tag, record} = await eventPromise;

    expect(tag).to.equal("test.hello");
    expect(record.data).to.equal("hello world!");

    await expect(clientPromise).to.eventually.be.fulfilled;

    await server.close();
  });

  it("should authenticate the client", async () => {
    const server = new FluentServer({
      security: {
        serverHostname: "test-server",
        sharedKey: "foo",
        authorize: false,
        userDict: {},
      },
    });

    await server.listen();

    const client = new FluentClient("test", {
      socket: {port: server.port || undefined, disableReconnect: true},
      security: {clientHostname: "test-client", sharedKey: "foo"},
    });

    const eventPromise = new Promise<{
      tag: protocol.Tag;
      time: protocol.Time;
      record: protocol.EventRecord;
    }>(resolve => {
      server.once("entry", (tag, time, record) => {
        resolve({tag, time, record});
      });
    });

    const clientPromise = client.emit("hello", {data: "hello world!"});

    const {tag, record} = await eventPromise;

    expect(tag).to.equal("test.hello");
    expect(record.data).to.equal("hello world!");

    await expect(clientPromise).to.eventually.be.fulfilled;

    await server.close();
  });

  it("should authenticate and authorize the client", async () => {
    const server = new FluentServer({
      security: {
        serverHostname: "test-server",
        sharedKey: "foo",
        authorize: true,
        userDict: {alice: "hunter2"},
      },
    });

    await server.listen();

    const client = new FluentClient("test", {
      socket: {port: server.port || undefined, disableReconnect: true},
      security: {
        clientHostname: "test-client",
        sharedKey: "foo",
        username: "alice",
        password: "hunter2",
      },
    });

    const eventPromise = new Promise<{
      tag: protocol.Tag;
      time: protocol.Time;
      record: protocol.EventRecord;
    }>(resolve => {
      server.once("entry", (tag, time, record) => {
        resolve({tag, time, record});
      });
    });

    const clientPromise = client.emit("hello", {data: "hello world!"});

    const {tag, record} = await eventPromise;

    expect(tag).to.equal("test.hello");
    expect(record.data).to.equal("hello world!");

    await expect(clientPromise).to.eventually.be.fulfilled;

    await server.close();
  });
});
