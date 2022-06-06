import {expect} from "chai";
import * as protocol from "../src/protocol";
import {EventTime} from "../src";
import {
  AuthError,
  ConfigError,
  DecodeError,
  SharedKeyMismatchError,
} from "../src/error";
import {Readable} from "stream";

describe("Protocol", () => {
  describe("isHelo", () => {
    it("should reject bad objects", () => {
      expect(protocol.isHelo({})).to.be.false;
      expect(protocol.isHelo(1234)).to.be.false;
      expect(protocol.isHelo(null)).to.be.false;
      expect(protocol.isHelo("testing")).to.be.false;
      expect(protocol.isHelo([])).to.be.false;
      expect(protocol.isHelo(["HELO"])).to.be.false;
      expect(protocol.isHelo(["PING"])).to.be.false;
      expect(protocol.isHelo(["HELO", 1])).to.be.false;
      expect(protocol.isHelo(["HELO", {nonce: 1}])).to.be.false;
      expect(protocol.isHelo(["HELO", {}])).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isHelo(["HELO", {nonce: ""}])).to.be.true;
      expect(protocol.isHelo(["HELO", {nonce: Buffer.alloc(0)}])).to.be.true;
      expect(protocol.isHelo(["HELO", {nonce: "", auth: ""}])).to.be.true;
      expect(protocol.isHelo(["HELO", {nonce: "", auth: Buffer.alloc(0)}])).to
        .be.true;
      expect(protocol.isHelo(["HELO", {nonce: "", auth: "", keepalive: true}]))
        .to.be.true;
    });
  });
  describe("isPing", () => {
    it("should reject bad objects", () => {
      expect(protocol.isPing({})).to.be.false;
      expect(protocol.isPing(1234)).to.be.false;
      expect(protocol.isPing(null)).to.be.false;
      expect(protocol.isPing("testing")).to.be.false;
      expect(protocol.isPing([])).to.be.false;
      expect(protocol.isPing(["PING"])).to.be.false;
      expect(protocol.isPong(["PONG"])).to.be.false;
      expect(protocol.isPing(["PING", 1, 2, 3, 4, 5])).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isPing(["PING", "", "", "", "", ""])).to.be.true;
      expect(protocol.isPing(["PING", "", Buffer.alloc(0), "", "", ""])).to.be
        .true;
    });
  });
  describe("isPong", () => {
    it("should reject bad objects", () => {
      expect(protocol.isPong({})).to.be.false;
      expect(protocol.isPong(1234)).to.be.false;
      expect(protocol.isPong(null)).to.be.false;
      expect(protocol.isPong("testing")).to.be.false;
      expect(protocol.isPong([])).to.be.false;
      expect(protocol.isPong(["PONG"])).to.be.false;
      expect(protocol.isPong(["PING"])).to.be.false;
      expect(protocol.isPong(["PONG", 1, 2, 3, 4])).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isPong(["PONG", true, "", "", ""])).to.be.true;
      expect(protocol.isPong(["PONG", false, "", "", ""])).to.be.true;
    });
  });
  describe("isAck", () => {
    it("should reject bad objects", () => {
      expect(protocol.isAck({})).to.be.false;
      expect(protocol.isAck(1234)).to.be.false;
      expect(protocol.isAck(null)).to.be.false;
      expect(protocol.isAck("testing")).to.be.false;
      expect(protocol.isAck([])).to.be.false;
      expect(protocol.isAck({ack: 10})).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isAck({ack: ""})).to.be.true;
    });
  });
  describe("isTime", () => {
    it("should reject bad objects", () => {
      expect(protocol.isTime({})).to.be.false;
      expect(protocol.isTime(null)).to.be.false;
      expect(protocol.isTime("testing")).to.be.false;
      expect(protocol.isTime([])).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isTime(1234)).to.be.true;
      expect(protocol.isTime(EventTime.now())).to.be.true;
    });
  });
  describe("isOption", () => {
    it("should reject bad objects", () => {
      expect(protocol.isOption(1234)).to.be.false;
      expect(protocol.isOption("testing")).to.be.false;
      expect(protocol.isOption({size: "abc"})).to.be.false;
      expect(protocol.isOption({size: 10, compressed: 10})).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isOption(null)).to.be.true;
      expect(protocol.isOption(undefined)).to.be.true;
      expect(protocol.isOption({})).to.be.true;
      expect(protocol.isOption({size: 10})).to.be.true;
      expect(protocol.isOption({chunk: "abc"})).to.be.true;
      expect(protocol.isOption({compressed: "abc", size: 10})).to.be.true;
    });
  });
  describe("isTag", () => {
    it("should reject bad objects", () => {
      expect(protocol.isTag(1234)).to.be.false;
      expect(protocol.isTag(null)).to.be.false;
      expect(protocol.isTag(undefined)).to.be.false;
      expect(protocol.isTag([])).to.be.false;
      expect(protocol.isTag({})).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isTag("tag")).to.be.true;
    });
  });
  describe("isEventRecord", () => {
    it("should reject bad objects", () => {
      expect(protocol.isEventRecord(1234)).to.be.false;
      expect(protocol.isEventRecord(null)).to.be.false;
      expect(protocol.isEventRecord(undefined)).to.be.false;
      expect(protocol.isEventRecord("testing")).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isEventRecord({})).to.be.true;
      expect(protocol.isEventRecord({a: "", b: 10})).to.be.true;
    });
  });
  describe("isEntry", () => {
    it("should reject bad objects", () => {
      expect(protocol.isEntry(1234)).to.be.false;
      expect(protocol.isEntry(null)).to.be.false;
      expect(protocol.isEntry(undefined)).to.be.false;
      expect(protocol.isEntry([])).to.be.false;
      expect(protocol.isEntry("testing")).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isEntry([0, {}])).to.be.true;
      expect(protocol.isEntry([EventTime.now(), {a: "", b: 10}])).to.be.true;
    });
  });
  describe("isMessageMode", () => {
    it("should reject bad objects", () => {
      expect(protocol.isMessageMode(1234)).to.be.false;
      expect(protocol.isMessageMode(null)).to.be.false;
      expect(protocol.isMessageMode(undefined)).to.be.false;
      expect(protocol.isMessageMode([])).to.be.false;
      expect(protocol.isMessageMode("testing")).to.be.false;
      expect(protocol.isMessageMode(["test", 0, null])).to.be.false;
      expect(protocol.isMessageMode(["test", 0, null, {}])).to.be.false;
      expect(protocol.isMessageMode(["test", null, {}])).to.be.false;
      expect(protocol.isMessageMode([1, null, {}])).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isMessageMode(["test", 0, {a: 1}])).to.be.true;
      expect(protocol.isMessageMode(["test", 0, {a: 1}, {}])).to.be.true;
      expect(protocol.isMessageMode(["test", EventTime.now(), {a: 1}, {}])).to
        .be.true;
    });
  });
  describe("isForwardMode", () => {
    it("should reject bad objects", () => {
      expect(protocol.isForwardMode(1234)).to.be.false;
      expect(protocol.isForwardMode(null)).to.be.false;
      expect(protocol.isForwardMode(undefined)).to.be.false;
      expect(protocol.isForwardMode([])).to.be.false;
      expect(protocol.isForwardMode("testing")).to.be.false;
      expect(protocol.isForwardMode(["test", null, {}])).to.be.false;
      expect(protocol.isForwardMode([1, null, {}])).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isForwardMode(["test", [[0, {a: 1}]]])).to.be.true;
      expect(protocol.isForwardMode(["test", [[0, {a: 1}]], {}])).to.be.true;
      expect(protocol.isForwardMode(["test", [[EventTime.now(), {a: 1}]], {}]))
        .to.be.true;
      expect(protocol.isForwardMode(["test", []])).to.be.true;
    });
  });
  describe("isPackedForwardMode", () => {
    it("should reject bad objects", () => {
      expect(protocol.isPackedForwardMode(1234)).to.be.false;
      expect(protocol.isPackedForwardMode(null)).to.be.false;
      expect(protocol.isPackedForwardMode(undefined)).to.be.false;
      expect(protocol.isPackedForwardMode([])).to.be.false;
      expect(protocol.isPackedForwardMode("testing")).to.be.false;
      expect(protocol.isPackedForwardMode(["test", null, {}])).to.be.false;
      expect(protocol.isPackedForwardMode([1, null, {}])).to.be.false;
      expect(protocol.isPackedForwardMode(["test", [], {}])).to.be.false;
      expect(
        protocol.isPackedForwardMode([
          "test",
          Buffer.alloc(0),
          {compressed: "gzip"},
        ])
      ).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(protocol.isPackedForwardMode(["test", Buffer.alloc(0)])).to.be
        .true;
      expect(
        protocol.isPackedForwardMode([
          "test",
          Buffer.alloc(0),
          {compressed: "text"},
        ])
      ).to.be.true;
    });
  });
  describe("isCompressedPackedForwardMode", () => {
    it("should reject bad objects", () => {
      expect(protocol.isCompressedPackedForwardMode(1234)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(null)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(undefined)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode([])).to.be.false;
      expect(protocol.isCompressedPackedForwardMode("testing")).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(["test", null, {}])).to.be
        .false;
      expect(protocol.isCompressedPackedForwardMode([1, null, {}])).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(["test", [], {}])).to.be
        .false;
      expect(protocol.isCompressedPackedForwardMode(["test", Buffer.alloc(0)]))
        .to.be.false;
      expect(
        protocol.isCompressedPackedForwardMode([
          "test",
          Buffer.alloc(0),
          {compressed: "text"},
        ])
      ).to.be.false;
    });
    it("should accept valid objects", () => {
      expect(
        protocol.isCompressedPackedForwardMode([
          "test",
          Buffer.alloc(0),
          {compressed: "gzip"},
        ])
      ).to.be.true;
    });
  });

  describe("generateHelo", () => {
    it("should generate helo", () => {
      const helo = protocol.generateHelo("abc", "", false);
      expect(protocol.isHelo(helo)).to.be.true;
      expect(protocol.parseHelo(helo).nonce).to.equal("abc");
      expect(protocol.parseHelo(helo).auth).to.equal("");
      expect(protocol.parseHelo(helo).keepalive).to.be.false;
    });
  });

  describe("generatePing", () => {
    const sharedKeyInfo: protocol.SharedKeyInfo = {
      key: "abc",
      salt: "abc",
      nonce: "abc",
    };
    it("should generate ping", () => {
      const ping = protocol.generatePing("abc", sharedKeyInfo);
      expect(protocol.isPing(ping)).to.be.true;
      expect(ping[4]).to.equal("");
      expect(ping[5]).to.equal("");
    });
    it("should add user info if provided", () => {
      const userInfo: protocol.ClientAuthInfo = {
        salt: "abc",
        username: "alice",
        password: "hunter2",
      };
      const ping = protocol.generatePing("abc", sharedKeyInfo, userInfo);
      expect(protocol.isPing(ping)).to.be.true;
      expect(ping[4]).to.equal(userInfo.username);
      expect(ping[5]).not.to.equal("");
    });
  });

  describe("checkPing", () => {
    const sharedKeyInfo: protocol.SharedKeyInfo = {
      key: "abc",
      salt: "abc",
      nonce: "abc",
    };
    const serverKeyInfo: protocol.ServerKeyInfo = {
      key: "abc",
      nonce: "abc",
    };
    const userInfo: protocol.ClientAuthInfo = {
      salt: "abc",
      username: "alice",
      password: "hunter2",
    };
    const authInfo: protocol.ServerAuthInfo = {
      salt: "abc",
      userDict: {alice: "hunter2"},
    };
    it("should reject if same hostname", () => {
      const testPing = protocol.generatePing("abc", sharedKeyInfo);
      const check = () => protocol.checkPing(testPing, "abc", serverKeyInfo);
      expect(check).to.throw(ConfigError, /Same hostname/);
    });

    it("should reject if bad digest", () => {
      const testPing = protocol.generatePing("abc", {
        ...sharedKeyInfo,
        key: "dummy",
      });
      const check = () => protocol.checkPing(testPing, "def", serverKeyInfo);
      expect(check).to.throw(SharedKeyMismatchError, /mismatch/);
    });

    it("should reject if auth info required but not provided", () => {
      const testPing = protocol.generatePing("abc", sharedKeyInfo);
      const check = () =>
        protocol.checkPing(testPing, "def", serverKeyInfo, authInfo);
      expect(check).to.throw(AuthError, /missing/);
    });

    it("should reject if user does not exist", () => {
      const testPing = protocol.generatePing("abc", sharedKeyInfo, {
        ...userInfo,
        username: "balice",
      });
      const check = () =>
        protocol.checkPing(testPing, "def", serverKeyInfo, authInfo);
      expect(check).to.throw(AuthError, /mismatch/);
    });

    it("should reject if password invalid", () => {
      const testPing = protocol.generatePing("abc", sharedKeyInfo, {
        ...userInfo,
        password: "hunter3",
      });
      const check = () =>
        protocol.checkPing(testPing, "def", serverKeyInfo, authInfo);
      expect(check).to.throw(AuthError, /mismatch/);
    });

    it("should accept valid data", () => {
      const testPing = protocol.generatePing("abc", sharedKeyInfo);
      const check = () => {
        const pingResult = protocol.checkPing(testPing, "def", serverKeyInfo);
        expect(pingResult.sharedKeyInfo).not.to.be.undefined;
        expect(pingResult.sharedKeyInfo?.salt).to.equal(sharedKeyInfo.salt);
      };
      expect(check).not.to.throw();
    });

    it("should accept valid data when authentication is required", () => {
      const testPing = protocol.generatePing("abc", sharedKeyInfo, userInfo);
      const check = () => {
        const pingResult = protocol.checkPing(
          testPing,
          "def",
          serverKeyInfo,
          authInfo
        );
        expect(pingResult.sharedKeyInfo).not.to.be.undefined;
        expect(pingResult.sharedKeyInfo?.salt).to.equal(sharedKeyInfo.salt);
      };
      expect(check).not.to.throw();
    });
  });

  describe("generatePong", () => {
    const sharedKeyInfo: protocol.SharedKeyInfo = {
      key: "abc",
      salt: "abc",
      nonce: "abc",
    };
    it("should generate pong for auth failure", () => {
      const pong = protocol.generatePong("abc", false, "failed");
      expect(protocol.isPong(pong)).to.be.true;
      expect(pong[1]).to.be.false;
      expect(pong[2]).to.equal("failed");
      expect(pong[3]).to.equal("abc");
      expect(pong[4]).to.equal("");
    });
    it("should generate pong for auth success", () => {
      const pong = protocol.generatePong("abc", true, "", sharedKeyInfo);
      expect(protocol.isPong(pong)).to.be.true;
      expect(pong[1]).to.be.true;
      expect(pong[2]).to.equal("");
      expect(pong[3]).to.equal("abc");
      expect(pong[4]).not.to.equal("");
    });
  });

  describe("checkPong", () => {
    const sharedKeyInfo: protocol.SharedKeyInfo = {
      key: "abc",
      salt: "abc",
      nonce: "abc",
    };
    it("should reject if same hostname", () => {
      const testPong = protocol.generatePong("abc", true, "", sharedKeyInfo);
      const check = () => protocol.checkPong(testPong, "abc", sharedKeyInfo);
      expect(check).to.throw(ConfigError, /Same hostname/);
    });
    it("should reject if auth failed", () => {
      const testPong = protocol.generatePong(
        "abc",
        false,
        "test fail",
        sharedKeyInfo
      );
      const check = () => protocol.checkPong(testPong, "def", sharedKeyInfo);
      expect(check).to.throw(AuthError, /test fail/);
    });
    it("should reject if bad digest", () => {
      const testPong = protocol.generatePong("abc", true, "", {
        ...sharedKeyInfo,
        key: "def",
      });
      const check = () => protocol.checkPong(testPong, "def", sharedKeyInfo);
      expect(check).to.throw(SharedKeyMismatchError, /shared key/);
    });
    it("should accept valid data", () => {
      const testPong = protocol.generatePong("abc", true, "", sharedKeyInfo);
      const check = () => protocol.checkPong(testPong, "def", sharedKeyInfo);
      expect(check).not.to.throw();
    });
  });

  describe("generateAck", () => {
    it("should generate ack", () => {
      const ack = protocol.generateAck("abc");
      expect(protocol.isAck(ack)).to.be.true;
      expect(ack.ack).to.equal("abc");
    });
  });

  describe("generateMessageMode", () => {
    it("should generate message", () => {
      const message = protocol.generateMessageMode("test", 0, {abc: "def"});
      expect(protocol.isMessageMode(message)).to.be.true;
      expect(protocol.isForwardMode(message)).to.be.false;
      expect(protocol.isPackedForwardMode(message)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.false;
    });
    it("should generate message with chunk", () => {
      const message = protocol.generateMessageMode(
        "test",
        0,
        {abc: "def"},
        "chunk"
      );
      expect(protocol.isMessageMode(message)).to.be.true;
      expect(protocol.isForwardMode(message)).to.be.false;
      expect(protocol.isPackedForwardMode(message)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.false;
      expect(message[3]?.chunk).to.equal("chunk");
    });
  });
  describe("generateForwardMode", () => {
    const entries = [
      protocol.generateEntry(0, {abc: "def"}),
      protocol.generateEntry(1, {ghi: "jkl"}),
    ];
    it("should generate message", () => {
      const message = protocol.generateForwardMode("test", entries);
      expect(protocol.isMessageMode(message)).to.be.false;
      expect(protocol.isForwardMode(message)).to.be.true;
      expect(protocol.isPackedForwardMode(message)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.false;
    });
    it("should generate message with chunk", () => {
      const message = protocol.generateForwardMode("test", entries, "chunk");
      expect(protocol.isMessageMode(message)).to.be.false;
      expect(protocol.isForwardMode(message)).to.be.true;
      expect(protocol.isPackedForwardMode(message)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.false;
      expect(message[2]?.chunk).to.equal("chunk");
    });
  });
  describe("generatePackedForwardMode", () => {
    const entries = [
      protocol.packEntry(protocol.generateEntry(0, {abc: "def"})),
      protocol.packEntry(protocol.generateEntry(1, {ghi: "jkl"})),
    ];
    const entryLength = entries.reduce((r, v) => r + v.length, 0);
    it("should generate message", () => {
      const message = protocol.generatePackedForwardMode(
        "test",
        entries,
        entryLength
      );
      expect(protocol.isMessageMode(message)).to.be.false;
      expect(protocol.isForwardMode(message)).to.be.false;
      expect(protocol.isPackedForwardMode(message)).to.be.true;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.false;
    });
    it("should generate message with chunk", () => {
      const message = protocol.generatePackedForwardMode(
        "test",
        entries,
        entryLength,
        "chunk"
      );
      expect(protocol.isMessageMode(message)).to.be.false;
      expect(protocol.isForwardMode(message)).to.be.false;
      expect(protocol.isPackedForwardMode(message)).to.be.true;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.false;
      expect(message[2]?.chunk).to.equal("chunk");
    });
  });
  describe("generateCompressedPackedForwardMode", () => {
    const entries = [
      protocol.packEntry(protocol.generateEntry(0, {abc: "def"})),
      protocol.packEntry(protocol.generateEntry(1, {ghi: "jkl"})),
    ];
    const entryLength = entries.reduce((r, v) => r + v.length, 0);
    it("should generate message", () => {
      const message = protocol.generateCompressedPackedForwardMode(
        "test",
        entries,
        entryLength
      );
      expect(protocol.isMessageMode(message)).to.be.false;
      expect(protocol.isForwardMode(message)).to.be.false;
      expect(protocol.isPackedForwardMode(message)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.true;
      expect(message[2]?.compressed).to.equal("gzip");
    });
    it("should generate message with chunk", () => {
      const message = protocol.generateCompressedPackedForwardMode(
        "test",
        entries,
        entryLength,
        "chunk"
      );
      expect(protocol.isMessageMode(message)).to.be.false;
      expect(protocol.isForwardMode(message)).to.be.false;
      expect(protocol.isPackedForwardMode(message)).to.be.false;
      expect(protocol.isCompressedPackedForwardMode(message)).to.be.true;
      expect(message[2]?.chunk).to.equal("chunk");
      expect(message[2]?.compressed).to.equal("gzip");
    });
  });
  describe("parseTransport", () => {
    const entries = [
      protocol.generateEntry(0, {abc: "def"}),
      protocol.generateEntry(1, {ghi: "jkl"}),
    ];
    const packedEntries = entries.map(protocol.packEntry);
    const packedEntryLength = packedEntries.reduce((r, v) => r + v.length, 0);
    it("should throw on bad message", () => {
      const badMessage = [
        "",
        [],
        {chunk: 10},
      ] as unknown as protocol.ClientTransportMessage;
      expect(() => protocol.parseTransport(badMessage)).to.throw(
        DecodeError,
        /Expected transport/
      );
    });
    it("should parse message mode", () => {
      const message = protocol.generateMessageMode("test", ...entries[0]);
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries[0]).to.deep.equal(entries[0]);
      expect(parsed.chunk).not.to.equal("chunk");
    });
    it("should parse message mode with chunk", () => {
      const message = protocol.generateMessageMode(
        "test",
        ...entries[0],
        "chunk"
      );
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries[0]).to.deep.equal(entries[0]);
      expect(parsed.chunk).to.equal("chunk");
    });
    it("should parse forward mode", () => {
      const message = protocol.generateForwardMode("test", entries);
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries).to.deep.equal(entries);
      expect(parsed.chunk).not.to.equal("chunk");
    });
    it("should parse forward mode with chunk", () => {
      const message = protocol.generateForwardMode("test", entries, "chunk");
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries).to.deep.equal(entries);
      expect(parsed.chunk).to.equal("chunk");
    });
    it("should parse packed forward mode", () => {
      const message = protocol.generatePackedForwardMode(
        "test",
        packedEntries,
        packedEntryLength
      );
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries).to.deep.equal(entries);
      expect(parsed.chunk).not.to.equal("chunk");
    });
    it("should parse packed forward mode with chunk", () => {
      const message = protocol.generatePackedForwardMode(
        "test",
        packedEntries,
        packedEntryLength,
        "chunk"
      );
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries).to.deep.equal(entries);
      expect(parsed.chunk).to.equal("chunk");
    });
    it("should parse compressed packed forward mode", () => {
      const message = protocol.generateCompressedPackedForwardMode(
        "test",
        packedEntries,
        packedEntryLength
      );
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries).to.deep.equal(entries);
      expect(parsed.chunk).not.to.equal("chunk");
    });
    it("should parse compressed packed forward mode with chunk", () => {
      const message = protocol.generateCompressedPackedForwardMode(
        "test",
        packedEntries,
        packedEntryLength,
        "chunk"
      );
      const parsed = protocol.parseTransport(message);
      expect(parsed.tag).to.equal("test");
      expect(parsed.entries).to.deep.equal(entries);
      expect(parsed.chunk).to.equal("chunk");
    });
  });
  describe("decodeEntries", () => {
    it("should reject invalid entries", () => {
      const entry = ["bad", "entry"];
      const packedEntry = protocol.encode(entry);
      expect(() => protocol.decodeEntries(packedEntry)).to.throw(
        DecodeError,
        /invalid entries/
      );
    });
  });
  describe("decodeClientStream", () => {
    it("should parse msgpack str encoded records", async () => {
      const entries = [
        protocol.generateEntry(0, {abc: "def"}),
        protocol.generateEntry(1, {ghi: "jkl"}),
      ];
      const packedEntries = entries.map(protocol.packEntry);
      const packedEntryLength = packedEntries.reduce((r, v) => r + v.length, 0);
      const message = protocol.generatePackedForwardMode(
        "test",
        packedEntries,
        packedEntryLength
      );
      const msg = protocol.encodeMessage(message);
      // 1 byte for the array byte (0x93, 1 byte for the fixstr containing "test"), then one byte for each of the str
      // Change the 0xc4 (bin8) representing the message to a 0xd9 (str8), this is the same format as FluentD sends data as
      msg[1 + 1 + message[0].length] = 0xd9;

      const s = new Readable();
      s.push(msg);
      s.push(null);

      const iterable = protocol.decodeClientStream(s);
      let parsedMessage: protocol.ClientMessage | undefined;
      for await (const msg of iterable) {
        parsedMessage = msg;
      }

      expect(parsedMessage).to.not.be.undefined;
      expect(protocol.isClientTransportMessage(parsedMessage)).to.be.true;
      if (!parsedMessage || !protocol.isClientTransportMessage(parsedMessage)) {
        return;
      }

      const originalMessage = protocol.parseTransport(parsedMessage);
      console.log(originalMessage);
      expect(originalMessage.entries[0][0]).to.equal(entries[0][0]);
      expect(originalMessage.entries[1][0]).to.equal(entries[1][0]);
    });
  });
});
