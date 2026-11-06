import {
  createExecutor,
  TaskCancelledError,
  ExecutorClosedError,
  type PriorityExecutor,
} from '@lp-labs/priority-executor';
const executor: PriorityExecutor = createExecutor({ concurrency: 2 });
const result: Promise<number> = executor.submit(
  ({ signal }) => {
    signal.throwIfAborted();
    return 1;
  },
  { priority: 1 },
);
// @ts-expect-error task priority must be numeric.
executor.submit(() => 1, { priority: 'urgent' });
void [result, TaskCancelledError, ExecutorClosedError];
