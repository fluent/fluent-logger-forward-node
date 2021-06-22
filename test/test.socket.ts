import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import {FluentSocket, FluentSocketOptions} from "../src/socket";
import * as protocol from "../src/protocol";
import * as sinon from "sinon";

import {SocketNotWritableError} from "../src/error";
import {fakeSocket} from "./helpers";

chai.use(chaiAsPromised);
const expect = chai.expect;

const createFluentSocket = (options?: FluentSocketOptions) => {
  const socket = new FluentSocket(options);
  const stream = fakeSocket();
  const connectStub = sinon.stub(socket, <any>"createSocket");
  connectStub.callsFake((onConnect: () => void) => {
    process.nextTick(onConnect);
    return stream.socket;
  });

  return {socket, stream, connectStub};
};

describe("FluentSocket", () => {
  it("should connect", () => {
    const socket = new FluentSocket({disableReconnect: true});
    const connectStub = sinon.stub(socket, <any>"createTcpSocket");
    connectStub.returns(fakeSocket().socket);

    socket.connect();

    sinon.assert.calledOnce(connectStub);
  });

  it("should connect with tls", () => {
    const socket = new FluentSocket({tls: {}, disableReconnect: true});
    const connectStub = sinon.stub(socket, <any>"createTlsSocket");
    connectStub.returns(fakeSocket().socket);

    socket.connect();

    sinon.assert.calledOnce(connectStub);
  });

  it("should emit writable on connect", done => {
    const {socket, connectStub} = createFluentSocket({disableReconnect: true});

    socket.on("writable", () => {
      done();
    });

    socket.connect();

    sinon.assert.calledOnce(connectStub);
  });

  it("should handle drain", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      disableReconnect: true,
    });

    sinon.stub(stream.socket, "write").returns(false);

    socket.connect();

    // On connected
    socket.once("writable", async () => {
      socket.once("writable", () => {
        done();
      });
      socket.write(Buffer.from("bla", "utf-8"));
      await expect(
        socket.write(Buffer.from("foo", "utf-8"))
      ).to.eventually.be.rejectedWith(SocketNotWritableError);

      stream.socket.emit("drain");
    });

    // can only get one stream
    sinon.assert.calledOnce(connectStub);
  });

  it("should parse messages", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      disableReconnect: true,
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);

    socket.on("ack", (chunk: string) => {
      expect(chunk).to.equal("chonk");
      done();
    });

    stream.writable.write(
      protocol.encodeMessage(protocol.generateAck("chonk"))
    );
  });

  it("should reject helos", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      disableReconnect: true,
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);

    socket.on("error", (error: Error) => {
      expect(error.name).to.equal("AuthError");
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    stream.writable.write(
      protocol.encodeMessage(protocol.generateHelo("", "", true))
    );
  });

  it("should reject bad messages", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      disableReconnect: true,
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);

    socket.on("error", (error: Error) => {
      expect(error.name).to.equal("ResponseError");
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    stream.writable.write(protocol.encode("TESTINGTON"));
  });

  it("should close on timeout", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      disableReconnect: true,
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);

    socket.on("error", (error: Error) => {
      expect(error.name).to.equal("SocketTimeoutError");
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    stream.socket.emit("timeout");
  });

  it("should reconnect on error if reconnect settings are provided", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      reconnect: {delay: 10},
    });

    expect((<any>socket).reconnectEnabled).to.be.true;

    socket.connect();

    const spy = sinon.spy(socket, "connect");

    socket.once("writable", () => {
      socket.closeAndReconnect();
      expect(stream.socket.destroyed).to.be.true;
      socket.once("writable", () => {
        sinon.assert.calledOnce(spy);
        sinon.assert.calledTwice(connectStub);
        done();
      });
    });
  });

  it("should not reconnect after disconnect", done => {
    const {socket, connectStub} = createFluentSocket({reconnect: {}});

    expect((<any>socket).reconnectEnabled).to.be.true;

    socket.connect();

    sinon.assert.calledOnce(connectStub);

    const spy = sinon.spy(socket, "connect");

    socket.on("close", () => {
      process.nextTick(() => {
        expect((<any>socket).reconnectTimeoutId).to.be.null;
        sinon.assert.notCalled(spy);
        done();
      });
    });
    socket.on("writable", () => {
      socket.close();
    });
  });

  it("should reject writes if socket is not writable", done => {
    const {socket, stream, connectStub} = createFluentSocket();

    socket.connect();

    sinon.assert.calledOnce(connectStub);

    socket.once("writable", async () => {
      sinon.stub(stream.socket, "writable").get(() => false);
      await expect(
        socket.write(Buffer.from("foo", "utf-8"))
      ).to.eventually.be.rejectedWith(SocketNotWritableError);
      done();
    });
  });
});
