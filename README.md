# @lp-labs/priority-executor

Bounded asynchronous task dispatch with stable priority ordering and cooperative cancellation.
Independent, framework-neutral implementation with zero runtime or peer dependencies.

## Install

```bash
npm install @lp-labs/priority-executor
```

ES2022 ESM/CommonJS and TypeScript declarations. CommonJS:
`const { createExecutor } = require('@lp-labs/priority-executor')`.

## Quickstart

```js
import { createExecutor } from '@lp-labs/priority-executor';

const executor = createExecutor({ concurrency: 2, maxQueue: 10 });
const slow = executor.submit(async ({ signal }) => {
  signal.throwIfAborted();
  await Promise.resolve();
  return 'normal';
});
const urgent = executor.submit(() => 'urgent', { priority: 10 });
const [first, second] = await Promise.all([slow, urgent]);
if (first !== 'normal' || second !== 'urgent') throw new Error('Task result mismatch');
executor.close();
```

## API

`createExecutor(options?: ExecutorOptions): PriorityExecutor`

| Option        | Default | Contract                                                            |
| ------------- | ------- | ------------------------------------------------------------------- |
| `concurrency` | `4`     | Positive safe integer running-task limit.                           |
| `maxQueue`    | `1000`  | Non-negative safe integer waiting-task limit; excludes active work. |

`submit<T>(task: (context: TaskContext) => T | PromiseLike<T>, options?): Promise<T>`

- Task context is `{ id, signal }`. IDs increase monotonically and are not reused.
- `priority` defaults to 0 and must be finite. Higher numbers run first; equal priority is FIFO.
- `signal?` accepts a native `AbortSignal`. Pre-aborted submissions reject without queueing.
- Work starts immediately when a slot is free. Otherwise it joins the bounded priority queue.
- Queue-full submissions reject with `RangeError`; no hidden waiting or dropping occurs.
- `TaskCancelledError` reports `taskId` and cancellation `cause`. A pre-aborted external signal
  has not been assigned a task ID and reports `0`.
- `ExecutorClosedError` rejects work submitted after shutdown.

```js
import { createExecutor } from '@lp-labs/priority-executor';

const executor = createExecutor({ concurrency: 1 });
let release;
const gate = new Promise((resolve) => {
  release = resolve;
});
const blocker = executor.submit(() => gate);
const low = executor.submit(() => 'low', { priority: 1 });
const high = executor.submit(() => 'high', { priority: 9 });
release();
await blocker;
if ((await high) !== 'high' || (await low) !== 'low') throw new Error('Priority order failed');
executor.close();
```

## Cancellation, shutdown and inspection

- `cancel(id, reason?): boolean` removes/rejects queued work or aborts running work.
- Queued cancellation settles immediately and releases its queue position.
- Running cancellation is cooperative: its slot stays occupied until the task settles. A task
  that ignores its signal may still complete successfully.
- `close(reason?)` is idempotent, permanently rejects new submissions, rejects queued tasks with
  `ExecutorClosedError`, and aborts active task signals. It does not await running work.
- `pending()` returns independent queued entries (priority/FIFO order) followed by active entries,
  each `{ id, priority, status }`.
- `running` is the active count; `closed` indicates permanent shutdown.

Always observe rejected promises and await retained active promises during application shutdown.
Cancellation cannot undo side effects or guarantee that arbitrary task code stops.

## Performance and limits

Queued ordering uses array insertion/sort and removal, not a heap: insertion/removal are O(Q) to
O(Q log Q), for Q queued tasks. Dispatch is O(1) from the head. Active state is O(C), and queued
state is O(Q). Keep limits appropriate for the workload.

This is a process-local scheduler: no worker threads, persistence, deadlines, retries, fairness
across tenants, queueing policies beyond priority/FIFO or hard preemption are provided.
Tasks execute on the current JavaScript runtime. Do not use client-side availability for
authorization.

Exported types: `TaskContext`, `Task`, `ExecutorOptions`, `TaskInfo`, `PriorityExecutor`.
Runtime exports: `createExecutor`, `TaskCancelledError`, `ExecutorClosedError`.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
