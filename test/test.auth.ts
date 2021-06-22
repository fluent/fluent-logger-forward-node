import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import {FluentAuthSocket, FluentAuthOptions} from "../src/auth";
import {CloseState, FluentSocketOptions} from "../src/socket";
import * as protocol from "../src/protocol";
import {fakeSocket} from "./helpers";

chai.use(chaiAsPromised);
const expect = chai.expect;

const sandbox = sinon.createSandbox();

const createFluentSocket = (
  authOptions: FluentAuthOptions,
  socketOptions?: FluentSocketOptions
) => {
  const socket = new FluentAuthSocket(authOptions, socketOptions);
  const stream = fakeSocket();
  const connectStub = sandbox.stub(
    FluentAuthSocket.prototype,
    <any>"createSocket"
  );
  connectStub.callsFake((onConnect: () => void) => {
    process.nextTick(onConnect);
    return stream.socket;
  });

  return {socket, stream, connectStub};
};

const serverHostname = "test-server";
const serverKeyInfo: protocol.ServerKeyInfo = {
  nonce: "nonce",
  key: "foobar",
};
const defaultAuthOptions: FluentAuthOptions = {
  clientHostname: "test-client",
  sharedKey: serverKeyInfo.key,
};

describe("FluentAuthSocket", () => {
  afterEach(() => {
    sandbox.restore();
  });

  it("should not be writable until after authentication", done => {
    const {socket, stream, connectStub} =
      createFluentSocket(defaultAuthOptions);

    // not writable until after we send pong
    let writableOk = false;

    socket.once("connected", () => {
      stream.writable.write(
        protocol.encode(protocol.generateHelo(serverKeyInfo.nonce, "", true))
      );
    });

    stream.readable.once("data", data => {
      const message = protocol.decodeClientMessage(data);
      expect(protocol.isPing(message)).to.be.true;

      const pingResult = protocol.checkPing(
        message as protocol.PingMessage,
        serverHostname,
        serverKeyInfo
      );
      stream.writable.write(
        protocol.encode(
          protocol.generatePong(
            serverHostname,
            true,
            "",
            pingResult.sharedKeyInfo
          )
        )
      );
      writableOk = true;
    });

    socket.once("writable", () => {
      expect(writableOk).to.be.true;
      expect(socket.writable()).to.be.true;
      done();
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);
  });

  it("should auth with username and password", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      ...defaultAuthOptions,
      username: "alice",
      password: "hunter2",
    });

    // not writable until after we send pong
    let writableOk = false;

    socket.once("connected", () => {
      stream.writable.write(
        protocol.encode(
          protocol.generateHelo(serverKeyInfo.nonce, "test", true)
        )
      );
    });

    stream.readable.once("data", data => {
      const message = protocol.decodeClientMessage(data);
      expect(protocol.isPing(message)).to.be.true;

      const pingResult = protocol.checkPing(
        message as protocol.PingMessage,
        serverHostname,
        serverKeyInfo,
        {salt: "test", userDict: {alice: "hunter2"}}
      );
      stream.writable.write(
        protocol.encode(
          protocol.generatePong(
            serverHostname,
            true,
            "",
            pingResult.sharedKeyInfo
          )
        )
      );
      writableOk = true;
    });

    socket.once("writable", () => {
      expect(writableOk).to.be.true;
      expect(socket.writable()).to.be.true;
      done();
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);
  });

  it("should reject other messages before helo", done => {
    const {socket, stream, connectStub} =
      createFluentSocket(defaultAuthOptions);

    let errorOk = false;
    socket.once("connected", () => {
      stream.writable.write(protocol.encode(protocol.generateAck("chonk")));
      errorOk = true;
    });

    socket.on("error", (error: Error) => {
      expect(errorOk).to.be.true;
      expect(error.name).to.equal("ResponseError");
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);
  });

  it("should reject other messages before pong", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      ...defaultAuthOptions,
    });

    let errorOk = false;

    socket.once("connected", () => {
      stream.writable.write(
        protocol.encode(protocol.generateHelo(serverKeyInfo.nonce, "", true))
      );
    });

    stream.readable.once("data", data => {
      const message = protocol.decodeClientMessage(data);
      expect(protocol.isPing(message)).to.be.true;

      expect(() =>
        protocol.checkPing(
          message as protocol.PingMessage,
          serverHostname,
          serverKeyInfo
        )
      ).not.to.throw;

      stream.writable.write(protocol.encode(protocol.generateAck("chonk")));
      errorOk = true;
    });

    socket.on("error", (error: Error) => {
      expect(errorOk).to.be.true;
      expect(error.name).to.equal("ResponseError");
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);
  });

  it("handle pong auth error", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      ...defaultAuthOptions,
    });

    // not writable until after we send pong
    let errorOk = false;

    socket.once("connected", () => {
      stream.writable.write(
        protocol.encode(protocol.generateHelo(serverKeyInfo.nonce, "", true))
      );
    });

    stream.readable.once("data", data => {
      const message = protocol.decodeClientMessage(data);
      expect(protocol.isPing(message)).to.be.true;

      expect(() =>
        protocol.checkPing(
          message as protocol.PingMessage,
          serverHostname,
          serverKeyInfo
        )
      ).not.to.throw;
      stream.writable.write(
        protocol.encode(protocol.generatePong(serverHostname, false, "nuuuuu"))
      );
      errorOk = true;
    });

    socket.on("error", (error: Error) => {
      expect(errorOk).to.be.true;
      expect(error.name).to.equal("AuthError");
      expect(error.message).to.match(/nuuuu/);
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);
  });

  it("should reject bad server key in pong", done => {
    const {socket, stream, connectStub} = createFluentSocket({
      ...defaultAuthOptions,
    });

    // not writable until after we send pong
    let errorOk = false;

    socket.once("connected", () => {
      stream.writable.write(
        protocol.encode(protocol.generateHelo(serverKeyInfo.nonce, "", true))
      );
    });

    stream.readable.once("data", data => {
      const message = protocol.decodeClientMessage(data);
      expect(protocol.isPing(message)).to.be.true;

      const pingResult = protocol.checkPing(
        message as protocol.PingMessage,
        serverHostname,
        serverKeyInfo
      );
      stream.writable.write(
        protocol.encode(
          protocol.generatePong(serverHostname, true, "", {
            ...pingResult.sharedKeyInfo,
            key: "barbaz",
          })
        )
      );
      errorOk = true;
    });

    socket.on("error", (error: Error) => {
      expect(errorOk).to.be.true;
      expect(error.name).to.equal("SharedKeyMismatchError");
      expect(stream.socket.destroyed).to.be.true;
      done();
    });

    socket.connect();
    sinon.assert.calledOnce(connectStub);
  });

  it("should re auth after reconnect", done => {
    const socket = new FluentAuthSocket(
      {...defaultAuthOptions},
      {reconnect: {delay: 10}}
    );
    let stream = fakeSocket();
    const connectStub = sandbox.stub(
      FluentAuthSocket.prototype,
      <any>"createSocket"
    );
    connectStub.callsFake((onConnect: () => void) => {
      process.nextTick(onConnect);
      return stream.socket;
    });

    let connectedCalls = 0,
      dataCalls = 0;

    socket.on("connected", () => {
      connectedCalls += 1;
      stream.writable.write(
        protocol.encode(protocol.generateHelo(serverKeyInfo.nonce, "", true))
      );
    });

    const dataHandler = (data: Buffer) => {
      dataCalls += 1;
      const message = protocol.decodeClientMessage(data);
      expect(protocol.isPing(message)).to.be.true;

      const pingResult = protocol.checkPing(
        message as protocol.PingMessage,
        serverHostname,
        serverKeyInfo
      );
      stream.writable.write(
        protocol.encode(
          protocol.generatePong(
            serverHostname,
            true,
            "",
            pingResult.sharedKeyInfo
          )
        )
      );
    };
    stream.readable.on("data", dataHandler);
    socket.on("error", console.log);

    socket.once("writable", () => {
      sinon.assert.calledOnce(connectStub);
      expect(connectedCalls).to.equal(1);
      expect(dataCalls).to.equal(1);
      const oldStream = stream;
      stream = fakeSocket();
      stream.readable.on("data", dataHandler);
      socket.close(CloseState.RECONNECT);
      // new stream
      socket.once("writable", () => {
        expect(oldStream.socket.destroyed).to.be.true;
        sinon.assert.calledTwice(connectStub);
        expect(connectedCalls).to.equal(2);
        expect(dataCalls).to.equal(2);
        done();
      });
    });

    socket.connect();
  });
});
