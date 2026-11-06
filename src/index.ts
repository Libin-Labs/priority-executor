export interface TaskContext {
  id: number;
  signal: AbortSignal;
}
export type Task<T> = (context: TaskContext) => T | PromiseLike<T>;
export interface ExecutorOptions {
  concurrency?: number;
  maxQueue?: number;
}
export interface TaskInfo {
  id: number;
  priority: number;
  status: 'queued' | 'running';
}
export interface PriorityExecutor {
  submit<T>(task: Task<T>, options?: { priority?: number; signal?: AbortSignal }): Promise<T>;
  cancel(id: number, reason?: unknown): boolean;
  close(reason?: unknown): void;
  pending(): TaskInfo[];
  readonly running: number;
  readonly closed: boolean;
}
export class TaskCancelledError extends Error {
  constructor(
    readonly taskId: number,
    reason: unknown,
  ) {
    super(`Task ${taskId} was cancelled.`, { cause: reason });
    this.name = 'TaskCancelledError';
  }
}
export class ExecutorClosedError extends Error {
  constructor() {
    super('Executor is closed.');
    this.name = 'ExecutorClosedError';
  }
}
interface Entry {
  id: number;
  priority: number;
  order: number;
  state: 'queued' | 'running';
  controller: AbortController;
  external: AbortSignal | undefined;
  forward: (() => void) | undefined;
  run: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
}
function capacity(value: number, name: string, minimum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum)
    throw new RangeError(`${name} must be a safe integer >= ${minimum}.`);
  return value;
}
export function createExecutor(options: ExecutorOptions = {}): PriorityExecutor {
  const maximum = capacity(options.concurrency ?? 4, 'concurrency', 1);
  const queueLimit = capacity(options.maxQueue ?? 1000, 'maxQueue', 0);
  const queued: Entry[] = [],
    active = new Map<number, Entry>();
  let nextId = 1,
    order = 0,
    closed = false;
  const detach = (entry: Entry) => {
    if (entry.external && entry.forward) entry.external.removeEventListener('abort', entry.forward);
    entry.forward = undefined;
  };
  const cancel = (entry: Entry, reason: unknown): boolean => {
    if (entry.state === 'queued') {
      const index = queued.indexOf(entry);
      if (index < 0) return false;
      queued.splice(index, 1);
      detach(entry);
      entry.reject(new TaskCancelledError(entry.id, reason));
      drain();
      return true;
    }
    if (!active.has(entry.id) || entry.controller.signal.aborted) return false;
    entry.controller.abort(reason);
    return true;
  };
  const finish = (entry: Entry) => {
    active.delete(entry.id);
    detach(entry);
    drain();
  };
  const drain = () => {
    while (!closed && active.size < maximum && queued.length) {
      const entry = queued.shift()!;
      entry.state = 'running';
      active.set(entry.id, entry);
      void entry.run().then(
        (value) => {
          finish(entry);
          entry.resolve(value);
        },
        (error: unknown) => {
          finish(entry);
          entry.reject(error);
        },
      );
    }
  };
  return {
    get running() {
      return active.size;
    },
    get closed() {
      return closed;
    },
    submit<T>(task: Task<T>, config: { priority?: number; signal?: AbortSignal } = {}) {
      if (typeof task !== 'function')
        return Promise.reject(new TypeError('Expected a task callback.'));
      if (closed) return Promise.reject(new ExecutorClosedError());
      const priority = config.priority ?? 0;
      if (typeof priority !== 'number' || !Number.isFinite(priority))
        return Promise.reject(new TypeError('Priority must be finite.'));
      const external = config.signal;
      if (
        external !== undefined &&
        (external === null ||
          typeof external.throwIfAborted !== 'function' ||
          typeof external.addEventListener !== 'function' ||
          typeof external.removeEventListener !== 'function')
      )
        return Promise.reject(new TypeError('Expected an AbortSignal.'));
      if (external?.aborted) return Promise.reject(new TaskCancelledError(0, external.reason));
      if (nextId === Number.MAX_SAFE_INTEGER || order === Number.MAX_SAFE_INTEGER)
        return Promise.reject(new RangeError('Task identity exhausted.'));
      if (active.size >= maximum && queued.length >= queueLimit)
        return Promise.reject(new RangeError('Task queue is full.'));
      return new Promise<T>((resolve, reject) => {
        const id = nextId++,
          controller = new AbortController();
        const entry: Entry = {
          id,
          priority,
          order: order++,
          state: 'queued',
          controller,
          external,
          forward: undefined,
          run: async () => {
            controller.signal.throwIfAborted();
            return task({ id, signal: controller.signal });
          },
          resolve: (value) => resolve(value as T),
          reject,
        };
        if (external) {
          entry.forward = () => cancel(entry, external.reason);
          external.addEventListener('abort', entry.forward, { once: true });
          if (external.aborted) entry.forward();
        }
        if (entry.state === 'queued') {
          queued.push(entry);
          queued.sort((a, b) => b.priority - a.priority || a.order - b.order);
          drain();
        }
      });
    },
    cancel(id, reason) {
      if (!Number.isSafeInteger(id) || id < 1) throw new RangeError('Task ID must be positive.');
      const entry = active.get(id) ?? queued.find((item) => item.id === id);
      return entry ? cancel(entry, reason) : false;
    },
    close(reason) {
      if (closed) return;
      closed = true;
      for (const entry of queued.splice(0)) {
        detach(entry);
        entry.reject(new ExecutorClosedError());
      }
      for (const entry of active.values()) entry.controller.abort(reason);
    },
    pending() {
      return [
        ...queued.map(({ id, priority }) => ({ id, priority, status: 'queued' as const })),
        ...[...active.values()].map(({ id, priority }) => ({
          id,
          priority,
          status: 'running' as const,
        })),
      ];
    },
  };
}
