const assert = require("assert");
const { finished } = require("stream");
const { EventEmitter } = require("events");

const s1 = new EventEmitter();
let cbCalls = 0;
const cleanup = finished(s1, () => cbCalls++);
assert.strictEqual(typeof cleanup, "function", "finished should return a cleanup function");
assert.strictEqual(s1.listenerCount("end"), 1);
assert.strictEqual(s1.listenerCount("finish"), 1);
assert.strictEqual(s1.listenerCount("close"), 1);
assert.strictEqual(s1.listenerCount("error"), 1);

cleanup();
assert.strictEqual(s1.listenerCount("end"), 0, "cleanup should remove end listener");
assert.strictEqual(s1.listenerCount("finish"), 0, "cleanup should remove finish listener");
assert.strictEqual(s1.listenerCount("close"), 0, "cleanup should remove close listener");
assert.strictEqual(s1.listenerCount("error"), 0, "cleanup should remove error listener");

s1.emit("end");
s1.emit("error", new Error("nope"));
assert.strictEqual(cbCalls, 0, "callback must not fire after cleanup");

const s2 = new EventEmitter();
let fired = 0;
finished(s2, (err) => {
  fired++;
  assert.strictEqual(err, undefined);
});
s2.emit("close");
s2.emit("close");
assert.strictEqual(fired, 1, "callback should fire exactly once");

const s3 = new EventEmitter();
const cleanup3 = finished(s3, { cleanup: true }, () => fired++);
assert.strictEqual(typeof cleanup3, "function", "options-arg form should return cleanup function");
s3.emit("error", new Error("x"));
assert.strictEqual(fired, 2, "callback from third arg should fire");

console.log("test_stream_finished_cleanup: OK");
