import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createExecutor, TaskCancelledError, ExecutorClosedError } from '../dist/esm/index.js';
const deferred = () => {
  let resolve;
  const promise = new Promise((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
};
test('higher priority is dispatched first and equal priority remains FIFO', async () => {
  const gate = deferred(),
    started = [],
    executor = createExecutor({ concurrency: 1 });
  const first = executor.submit(async () => {
    started.push('blocker');
    await gate.promise;
    return 'done';
  });
  await Promise.resolve();
  const low = executor.submit(
    () => {
      started.push('low');
      return 'l';
    },
    { priority: -1 },
  );
  const high = executor.submit(
    () => {
      started.push('high-1');
      return 'h1';
    },
    { priority: 2 },
  );
  const next = executor.submit(
    () => {
      started.push('high-2');
      return 'h2';
    },
    { priority: 2 },
  );
  gate.resolve();
  await Promise.all([first, low, high, next]);
  assert.deepEqual(started, ['blocker', 'high-1', 'high-2', 'low']);
});
test('result and thrown task errors settle only their own submitted promise', async () => {
  const executor = createExecutor(),
    error = new Error('task error');
  assert.equal(await executor.submit(() => 42), 42);
  await assert.rejects(
    executor.submit(() => {
      throw error;
    }),
    (caught) => caught === error,
  );
  assert.equal(executor.running, 0);
});
test('priority and submission data are bounded and queue saturation is explicit', async () => {
  const gate = deferred(),
    executor = createExecutor({ concurrency: 1, maxQueue: 1 });
  const active = executor.submit(() => gate.promise);
  await Promise.resolve();
  const queued = executor.submit(() => 2);
  await assert.rejects(
    executor.submit(() => 3),
    RangeError,
  );
  await assert.rejects(
    executor.submit(() => 4, { priority: Infinity }),
    TypeError,
  );
  gate.resolve();
  await Promise.all([active, queued]);
  assert.throws(() => createExecutor({ concurrency: 0 }), RangeError);
  assert.throws(() => createExecutor({ maxQueue: -1 }), RangeError);
});
test('queued cancellation removes work, rejects with cause and dispatches next item', async () => {
  const gate = deferred(),
    executor = createExecutor({ concurrency: 1 });
  const active = executor.submit(() => gate.promise);
  const cancelled = executor.submit(() => assert.fail('cancelled task ran'));
  const next = executor.submit(() => 'next');
  assert.equal(executor.cancel(2, 'not needed'), true);
  await assert.rejects(
    cancelled,
    (error) => error instanceof TaskCancelledError && error.cause === 'not needed',
  );
  gate.resolve();
  await active;
  assert.equal(await next, 'next');
  assert.equal(executor.cancel(2), false);
});
test('running cancellation is cooperative and keeps the slot reserved until settlement', async () => {
  const gate = deferred(),
    executor = createExecutor({ concurrency: 1 });
  let signal;
  const running = executor.submit(({ signal: received }) => {
    signal = received;
    return gate.promise;
  });
  await Promise.resolve();
  assert.equal(executor.cancel(1, 'stop'), true);
  assert.equal(signal.reason, 'stop');
  const queued = executor.submit(() => 'later');
  assert.equal(executor.running, 1);
  gate.resolve('finished');
  assert.equal(await running, 'finished');
  assert.equal(await queued, 'later');
});
test('external queued abort propagates and listener ownership ends after settlement', async () => {
  const gate = deferred(),
    controller = new AbortController(),
    executor = createExecutor({ concurrency: 1 });
  const active = executor.submit(() => gate.promise);
  const queued = executor.submit(() => assert.fail('aborted task ran'), {
    signal: controller.signal,
  });
  controller.abort('external');
  await assert.rejects(
    queued,
    (error) => error instanceof TaskCancelledError && error.cause === 'external',
  );
  gate.resolve();
  await active;
  assert.equal(executor.pending().length, 0);
});
test('close rejects queued work and aborts active tasks without claiming they settled', async () => {
  const gate = deferred(),
    executor = createExecutor({ concurrency: 1 });
  let signal;
  const active = executor.submit(({ signal: received }) => {
    signal = received;
    return gate.promise;
  });
  await Promise.resolve();
  const queued = executor.submit(() => 2);
  executor.close('shutdown');
  assert.equal(executor.closed, true);
  assert.equal(signal.reason, 'shutdown');
  await assert.rejects(queued, ExecutorClosedError);
  await assert.rejects(
    executor.submit(() => 3),
    ExecutorClosedError,
  );
  assert.equal(executor.running, 1);
  gate.resolve(1);
  assert.equal(await active, 1);
});
test('queued external abort before dispatch never invokes user work', async () => {
  const gate = deferred(),
    controller = new AbortController(),
    executor = createExecutor({ concurrency: 1 });
  const active = executor.submit(() => gate.promise);
  const queued = executor.submit(() => assert.fail('aborted task ran'), {
    signal: controller.signal,
  });
  controller.abort(new Error('cancel'));
  await assert.rejects(queued, /cancel/);
  gate.resolve();
  await active;
});
test('pending snapshots separate running and queued tasks and cannot mutate internals', async () => {
  const gate = deferred(),
    executor = createExecutor({ concurrency: 1 });
  const active = executor.submit(() => gate.promise);
  await Promise.resolve();
  const queued = executor.submit(() => 1, { priority: 7 });
  const snapshot = executor.pending();
  assert.deepEqual(
    snapshot.map((row) => row.status),
    ['queued', 'running'],
  );
  snapshot[0].priority = 999;
  gate.resolve();
  await Promise.all([active, queued]);
});
test('concurrent stress never exceeds concurrency and cleans every entry', async () => {
  const executor = createExecutor({ concurrency: 4, maxQueue: 100 });
  let active = 0,
    peak = 0;
  const results = await Promise.all(
    Array.from({ length: 60 }, (_, i) =>
      executor.submit(async () => {
        active++;
        peak = Math.max(peak, active);
        await Promise.resolve();
        active--;
        return i;
      }),
    ),
  );
  assert.equal(peak, 4);
  assert.deepEqual(
    results,
    Array.from({ length: 60 }, (_, i) => i),
  );
  assert.equal(executor.running, 0);
  assert.deepEqual(executor.pending(), []);
});
test('CommonJS exposes independently constructed executors', async () => {
  const api = createRequire(import.meta.url)('../dist/cjs/index.js');
  assert.equal(await api.createExecutor().submit(() => 3), 3);
});
