import {expect} from "chai";
import EventTime from "../src/event_time";
import * as protocol from "../src/protocol";

describe("EventTime", () => {
  it("should equal the decoded value", () => {
    const eventTime = EventTime.now();
    const decoded = protocol.decode(protocol.encode(eventTime));
    expect(JSON.stringify(decoded)).to.equal(JSON.stringify(eventTime));
  });

  it("should equal fromDate and fromTimestamp", () => {
    const now = new Date(1489543720999); // 2017-03-15T02:08:40.999Z
    const timestamp = now.getTime();
    const eventTime = JSON.stringify(new EventTime(1489543720, 999000000));
    const eventTime1 = JSON.stringify(EventTime.fromDate(now));
    const eventTime2 = JSON.stringify(EventTime.fromTimestamp(timestamp));
    expect(eventTime1).to.equal(eventTime);
    expect(eventTime2).to.equal(eventTime);
  });
});
