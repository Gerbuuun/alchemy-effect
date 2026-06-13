import * as workflows from "@distilled.cloud/cloudflare/workflows";
import type { ConfigError } from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { AlchemyContext } from "../../AlchemyContext.ts";
import { ExecutionContext } from "../../ExecutionContext.ts";
import { ALCHEMY_PHASE } from "../../Phase.ts";
import type { PlatformServices } from "../../Platform.ts";
import * as Provider from "../../Provider.ts";
import { Resource } from "../../Resource.ts";
import { effectClass, taggedFunction } from "../../Util/effect.ts";
import { CloudflareEnvironment } from "../CloudflareEnvironment.ts";
import { Worker, WorkerEnvironment, type WorkerServices } from "./Worker.ts";

type WorkflowTypeId = "Cloudflare.Workflow";
const WorkflowTypeId: WorkflowTypeId = "Cloudflare.Workflow";

// ---------------------------------------------------------------------------
// Runtime services -- provided by the bridge when the workflow executes
// ---------------------------------------------------------------------------

/**
 * Service that carries the current workflow event payload.
 * `yield* WorkflowEvent` inside a workflow body to access it.
 */
export class WorkflowEvent extends Context.Service<
  WorkflowEvent,
  {
    payload: unknown;
    timestamp: Date;
    instanceId: string;
    workflowName: string;
    schedule?: WorkflowCronSchedule;
  }
>()("Cloudflare.WorkflowEvent") {}

export interface WorkflowCronSchedule {
  cron: string;
  scheduledTime: number;
}

export type WorkflowBackoff = "constant" | "linear" | "exponential";

export interface WorkflowStepConfig {
  retries?: {
    limit: number;
    delay: string | number;
    backoff?: WorkflowBackoff;
  };
  timeout?: string | number;
}

export interface WorkflowStepContextData {
  step: {
    name: string;
    count: number;
  };
  attempt: number;
  config: WorkflowStepConfig;
}

/**
 * Runtime information for the current `task` attempt.
 */
export class WorkflowStepContext extends Context.Service<
  WorkflowStepContext,
  WorkflowStepContextData
>()("Cloudflare.WorkflowStepContext") {}

export interface WorkflowRollbackContext<Output = unknown> {
  error: Error;
  output: Output | undefined;
}

export interface WorkflowRollbackOptions<Output = unknown, R = never> {
  rollback: (
    context: WorkflowRollbackContext<Output>,
  ) => Effect.Effect<void, never, R>;
  rollbackConfig?: WorkflowStepConfig;
}

export interface WorkflowWaitForEventOptions {
  type: string;
  timeout?: string | number;
}

type ExcludeWorkflowStepContext<R> = R extends {
  readonly key: "Cloudflare.WorkflowStepContext";
}
  ? never
  : R;

/**
 * Internal service that wraps the Cloudflare `WorkflowStep` object.
 * Not accessed directly by users -- use `task`, `sleep`, `sleepUntil` instead.
 */
export class WorkflowStep extends Context.Service<
  WorkflowStep,
  {
    do<T>(
      name: string,
      effect: Effect.Effect<T, never, any>,
      config?: WorkflowStepConfig,
      options?: WorkflowRollbackOptions<T>,
    ): Effect.Effect<T>;
    sleep(name: string, duration: string | number): Effect.Effect<void>;
    sleepUntil(name: string, timestamp: Date | number): Effect.Effect<void>;
    waitForEvent<T>(
      name: string,
      options: WorkflowWaitForEventOptions,
    ): Effect.Effect<T>;
  }
>()("Cloudflare.WorkflowStep") {}

// ---------------------------------------------------------------------------
// User-facing step primitives
// ---------------------------------------------------------------------------

/**
 * Execute a named, durable workflow step. The effect is run inside the
 * Cloudflare step transaction so its result is automatically persisted
 * and replayed on retries.
 *
 * Any services the inner effect requires (e.g. `WorkerEnvironment` from a
 * binding like `kv.put` / `kv.get`) are threaded through automatically by
 * capturing the surrounding workflow body's context and providing it to
 * the inner effect before it runs inside `step.do`.
 */
export function task<T, R = never>(
  name: string,
  effect: Effect.Effect<T, never, R>,
): Effect.Effect<T, never, WorkflowStep | ExcludeWorkflowStepContext<R>>;
export function task<T, R = never>(
  name: string,
  config: WorkflowStepConfig,
  effect: Effect.Effect<T, never, R>,
): Effect.Effect<T, never, WorkflowStep | ExcludeWorkflowStepContext<R>>;
export function task<T, R = never, RollbackReq = never>(
  name: string,
  effect: Effect.Effect<T, never, R>,
  options: WorkflowRollbackOptions<T, RollbackReq>,
): Effect.Effect<
  T,
  never,
  WorkflowStep | ExcludeWorkflowStepContext<R | RollbackReq>
>;
export function task<T, R = never, RollbackReq = never>(
  name: string,
  config: WorkflowStepConfig,
  effect: Effect.Effect<T, never, R>,
  options: WorkflowRollbackOptions<T, RollbackReq>,
): Effect.Effect<
  T,
  never,
  WorkflowStep | ExcludeWorkflowStepContext<R | RollbackReq>
>;
export function task<T, R = never, RollbackReq = never>(
  name: string,
  arg1: WorkflowStepConfig | Effect.Effect<T, never, R>,
  arg2?: Effect.Effect<T, never, R> | WorkflowRollbackOptions<T, RollbackReq>,
  arg3?: WorkflowRollbackOptions<T, RollbackReq>,
): Effect.Effect<
  T,
  never,
  WorkflowStep | ExcludeWorkflowStepContext<R | RollbackReq>
> {
  return Effect.gen(function* () {
    const step = yield* WorkflowStep;
    const context =
      yield* Effect.context<ExcludeWorkflowStepContext<R | RollbackReq>>();
    const hasConfig = !Effect.isEffect(arg1 as any);
    const config = hasConfig ? (arg1 as WorkflowStepConfig) : undefined;
    const effect = (hasConfig ? arg2 : arg1) as Effect.Effect<T, never, R>;
    const options = (hasConfig ? arg3 : arg2) as
      | WorkflowRollbackOptions<T, RollbackReq>
      | undefined;
    const rollback = options
      ? {
          ...options,
          rollback: (rollbackContext: WorkflowRollbackContext<T>) =>
            options.rollback(rollbackContext).pipe(Effect.provide(context)),
        }
      : undefined;
    return yield* step.do(
      name,
      effect.pipe(Effect.provide(context)),
      config,
      rollback as WorkflowRollbackOptions<T> | undefined,
    );
  });
}

/**
 * Pause the workflow for the given duration.
 */
export const sleep = (
  name: string,
  duration: string | number,
): Effect.Effect<void, never, WorkflowStep> =>
  Effect.gen(function* () {
    const step = yield* WorkflowStep;
    yield* step.sleep(name, duration);
  }).pipe(Effect.orDie);

/**
 * Pause the workflow until the given timestamp.
 */
export const sleepUntil = (
  name: string,
  timestamp: Date | number,
): Effect.Effect<void, never, WorkflowStep> =>
  Effect.gen(function* () {
    const step = yield* WorkflowStep;
    yield* step.sleepUntil(name, timestamp);
  }).pipe(Effect.orDie);

/**
 * Pause the workflow until an external event is delivered with
 * `WorkflowInstance.sendEvent`.
 */
export const waitForEvent = <T = unknown>(
  name: string,
  options: WorkflowWaitForEventOptions,
): Effect.Effect<T, never, WorkflowStep> =>
  Effect.gen(function* () {
    const step = yield* WorkflowStep;
    return yield* step.waitForEvent<T>(name, options);
  }).pipe(Effect.orDie);

/**
 * The services available inside a workflow run body.
 *
 * `WorkerEnvironment` is provided to the body at runtime by the workflow
 * export wrapper (see `make(env)` below), so users can access env bindings
 * from inside workflow steps via `yield* WorkerEnvironment` — the type must
 * reflect that or `yield* WorkerEnvironment` fails to type-check inside a
 * body even though it succeeds at runtime.
 *
 * `ExecutionContext` (scope + cache) is provided per run-invocation by
 * `WorkflowBridge.run` and threaded into every `task` via the surrounding
 * body context, so `@binding` helpers that need it (e.g. `Drizzle.postgres`)
 * resolve their per-run resources inside workflow steps just as they do in a
 * Worker `fetch`/`queue` handler.
 */
export type WorkflowRunServices =
  | WorkflowEvent
  | WorkflowStep
  | WorkflowStepContext
  | WorkerServices
  | ExecutionContext;

export type WorkflowServices = WorkflowRunServices | PlatformServices;

/**
 * Metadata stored in the worker export map to distinguish workflow exports
 * from durable object exports at bundle-generation time.
 */
export interface WorkflowExport {
  readonly kind: "workflow";
  readonly make: (env: unknown) => Effect.Effect<WorkflowImpl<any, any>>;
}

/**
 * A workflow implementation is a function from a typed `Input` payload to
 * an Effect that produces the workflow's `Result`. The Effect requires
 * `WorkflowRunServices` (event + step + env) to execute.
 */
export type WorkflowImpl<Input = unknown, Result = unknown> = (
  input: Input,
) => Effect.Effect<Result, never, WorkflowServices>;

export const isWorkflowExport = (value: unknown): value is WorkflowExport =>
  typeof value === "object" &&
  value !== null &&
  "kind" in value &&
  (value as any).kind === "workflow";

/**
 * Type guard for workflow binding metadata in the Worker binding contract.
 */
export const isWorkflowBinding = (binding: {
  type: string;
}): binding is {
  type: "workflow";
  name: string;
  workflowName: string;
  className: string;
  scriptName?: string;
} => binding.type === "workflow";

/**
 * Handle returned to the caller at deploy/bind time. Allows starting
 * workflow instances and checking their status from the Api layer.
 */
export interface WorkflowHandle<Input = unknown, Result = unknown> {
  Type: WorkflowTypeId;
  name: string;
  create(input: Input): Effect.Effect<WorkflowInstance<Result>>;
  create(
    options?: WorkflowInstanceCreateOptions<Input>,
  ): Effect.Effect<WorkflowInstance<Result>>;
  createBatch(
    batch: WorkflowInstanceCreateOptions<Input>[],
  ): Effect.Effect<WorkflowInstance<Result>[]>;
  get(instanceId: string): Effect.Effect<WorkflowInstance<Result>>;
}

export interface WorkflowInstanceCreateOptions<Input = unknown> {
  id?: string;
  params?: Input;
  retention?: WorkflowInstanceRetention;
}

export interface WorkflowInstanceRetention {
  successRetention?: string | number;
  errorRetention?: string | number;
}

export interface WorkflowInstance<Result = unknown> {
  id: string;
  status(): Effect.Effect<WorkflowInstanceStatus<Result>>;
  pause(): Effect.Effect<void>;
  resume(): Effect.Effect<void>;
  restart(options?: WorkflowInstanceRestartOptions): Effect.Effect<void>;
  terminate(): Effect.Effect<void>;
  sendEvent<Event = unknown>(
    event: WorkflowInstanceEvent<Event>,
  ): Effect.Effect<void>;
}

export interface WorkflowInstanceRestartOptions {
  from?: {
    name: string;
    count?: number;
    type?: "do" | "sleep" | "waitForEvent";
  };
}

export interface WorkflowInstanceEvent<Payload = unknown> {
  type: string;
  payload?: Payload;
}

export interface WorkflowInstanceStatus<Result = unknown> {
  status:
    | "queued"
    | "running"
    | "paused"
    | "errored"
    | "terminated"
    | "complete"
    | "waiting"
    | "waitingForPause"
    | "unknown"
    | (string & {});
  output?: Result;
  error?: { name: string; message: string } | null;
  rollback?: {
    outcome: "complete" | "failed";
    error: { name: string; message: string } | null;
  } | null;
}

export interface WorkflowClass extends Effect.Effect<
  WorkflowHandle,
  never,
  WorkflowHandle
> {
  <_Self>(): {
    <Input = unknown, Result = unknown, InitReq = never>(
      name: string,
      impl: Effect.Effect<WorkflowImpl<Input, Result>, ConfigError, InitReq>,
    ): Effect.Effect<
      WorkflowHandle<Input, Result>,
      never,
      Worker | Exclude<InitReq, WorkflowServices>
    > & {
      new (_: never): WorkflowImpl<Input, Result>;
    };
  };
  <Input = unknown, Result = unknown, InitReq = never>(
    name: string,
    impl: Effect.Effect<WorkflowImpl<Input, Result>, ConfigError, InitReq>,
  ): Effect.Effect<
    WorkflowHandle<Input, Result>,
    never,
    Worker | Exclude<InitReq, WorkflowServices>
  >;
}

export class WorkflowScope extends Context.Service<
  WorkflowScope,
  WorkflowHandle
>()("Cloudflare.Workflow") {}

/**
 * A Cloudflare Workflow that orchestrates durable, multi-step tasks with
 * automatic retries and at-least-once delivery.
 *
 * A Workflow follows the same two-phase pattern as Workers and Durable
 * Objects. The outer `Effect.gen` resolves shared dependencies. The inner
 * `Effect.fn` is the workflow body — a function from a typed `input`
 * payload to an Effect that runs steps using `task`, `sleep`, and
 * `sleepUntil`.
 *
 * ```typescript
 * Effect.gen(function* () {
 *   // Phase 1: resolve dependencies
 *   const notifier = yield* NotificationService;
 *
 *   return Effect.fn(function* (input: { orderId: string }) {
 *     // Phase 2: workflow body (durable steps)
 *     const result = yield* Cloudflare.task("process", doWork(input.orderId));
 *     yield* Cloudflare.sleep("cooldown", "10 seconds");
 *     return result;
 *   });
 * })
 * ```
 *
 * @resource
 *
 * @section Defining a Workflow
 * @example Minimal workflow
 * ```typescript
 * export default class MyWorkflow extends Cloudflare.Workflow<MyWorkflow>()(
 *   "MyWorkflow",
 *   Effect.gen(function* () {
 *     return Effect.fn(function* (input: { name: string }) {
 *       return { received: input.name };
 *     });
 *   }),
 * ) {}
 * ```
 *
 * @section Step Primitives
 * @example Running a named task
 * ```typescript
 * const result = yield* Cloudflare.task(
 *   "process-order",
 *   Effect.succeed({ orderId: "abc", total: 42 }),
 * );
 * ```
 *
 * @example Configuring retries and reading step context
 * ```typescript
 * const result = yield* Cloudflare.task(
 *   "call-api",
 *   { retries: { limit: 3, delay: "5 seconds", backoff: "linear" } },
 *   Effect.gen(function* () {
 *     const context = yield* Cloudflare.WorkflowStepContext;
 *     return { attempt: context.attempt };
 *   }),
 * );
 * ```
 *
 * @example Registering rollback
 * ```typescript
 * yield* Cloudflare.task(
 *   "reserve-inventory",
 *   reserveInventory,
 *   {
 *     rollback: ({ output }) =>
 *       output ? releaseInventory(output.reservationId) : Effect.void,
 *     rollbackConfig: { retries: { limit: 3, delay: "10 seconds" } },
 *   },
 * );
 * ```
 *
 * @example Sleeping between steps
 * ```typescript
 * yield* Cloudflare.sleep("cooldown", "30 seconds");
 * ```
 *
 * @example Waiting for an external event
 * ```typescript
 * const event = yield* Cloudflare.waitForEvent<{ approved: boolean }>(
 *   "approval",
 *   { type: "approval", timeout: "1 day" },
 * );
 * ```
 *
 * @example Accessing env bindings inside a task
 * Bind a resource (e.g. `KVNamespace`, `R2Bucket`) in the workflow's
 * outer init phase to get a typed Effect-native client, then use it
 * directly inside `task`. `task` threads the binding's service
 * requirement (`WorkerEnvironment`) through automatically so the inner
 * Effect needs no extra plumbing.
 *
 * ```typescript
 * Effect.gen(function* () {
 *   const kv = yield* Cloudflare.KVNamespace.bind(KV);
 *
 *   return Effect.fn(function* (input: { roomId: string; message: string }) {
 *     const { roomId, message } = input;
 *
 *     const stored = yield* Cloudflare.task(
 *       "kv-roundtrip",
 *       Effect.gen(function* () {
 *         const key = `workflow:${roomId}`;
 *         yield* kv.put(key, message);
 *         return yield* kv.get(key);
 *       }).pipe(Effect.orDie),
 *     );
 *
 *     return stored;
 *   });
 * });
 * ```
 *
 * @section Starting and Monitoring Instances
 * @example Creating an instance from a Worker
 * ```typescript
 * const workflow = yield* MyWorkflow;
 * const instance = yield* workflow.create({ orderId: "abc" });
 * ```
 *
 * @example Creating an instance with options
 * ```typescript
 * const instance = yield* workflow.create({
 *   id: "order-abc",
 *   params: { orderId: "abc" },
 *   retention: { successRetention: "1 day", errorRetention: "7 days" },
 * });
 * ```
 *
 * @example Creating a batch
 * ```typescript
 * const instances = yield* workflow.createBatch([
 *   { id: "order-a", params: { orderId: "a" } },
 *   { id: "order-b", params: { orderId: "b" } },
 * ]);
 * ```
 *
 * @example Checking instance status
 * ```typescript
 * const workflow = yield* MyWorkflow;
 * const handle = yield* workflow.get(instanceId);
 * const status = yield* handle.status();
 * ```
 *
 * @example Sending events and restarting instances
 * ```typescript
 * const instance = yield* workflow.get(instanceId);
 * yield* instance.sendEvent({ type: "approval", payload: { approved: true } });
 * yield* instance.restart({ from: { name: "approval", type: "waitForEvent" } });
 * ```
 *
 * @section Triggering from a Worker
 * Wire the workflow into HTTP routes so callers can fire instances
 * and poll for completion.
 *
 * @example Workflow start + status routes
 * ```typescript
 * // src/worker.ts
 * const notifier = yield* MyWorkflow;
 *
 * return {
 *   fetch: Effect.gen(function* () {
 *     const request = yield* HttpServerRequest;
 *
 *     if (request.url.startsWith("/workflow/start/")) {
 *       const id = request.url.split("/").pop()!;
 *       const instance = yield* notifier.create({ id });
 *       return HttpServerResponse.json({ instanceId: instance.id });
 *     }
 *
 *     if (request.url.startsWith("/workflow/status/")) {
 *       const id = request.url.split("/").pop()!;
 *       const instance = yield* notifier.get(id);
 *       return HttpServerResponse.json(yield* instance.status());
 *     }
 *
 *     return HttpServerResponse.text("Not Found", { status: 404 });
 *   }),
 * };
 * ```
 *
 * @section Testing Workflows
 * Workflows run asynchronously, so tests start an instance and
 * poll until it reaches a terminal status. A simple recipe with
 * `alchemy/Test/Bun`:
 *
 * @example Polling for workflow completion
 * ```typescript
 * test(
 *   "workflow completes",
 *   Effect.gen(function* () {
 *     const { url } = yield* stack;
 *
 *     const start = yield* HttpClient.post(`${url}/workflow/start/x`);
 *     const { instanceId } = (yield* start.json) as { instanceId: string };
 *
 *     let status: { status: string } | undefined;
 *     const deadline = Date.now() + 60_000;
 *     while (Date.now() < deadline) {
 *       const res = yield* HttpClient.get(
 *         `${url}/workflow/status/${instanceId}`,
 *       );
 *       status = (yield* res.json) as { status: string };
 *       if (status.status === "complete" || status.status === "errored") {
 *         break;
 *       }
 *       yield* Effect.sleep("2 seconds");
 *     }
 *
 *     expect(status?.status).toBe("complete");
 *   }),
 *   { timeout: 120_000 },
 * );
 * ```
 */
export const Workflow: WorkflowClass = taggedFunction(WorkflowScope, ((
  ...args: [] | [name: string, impl: Effect.Effect<WorkflowImpl<any, any>>]
) =>
  args.length === 0
    ? Workflow
    : effectClass(
        Effect.gen(function* () {
          const [name, impl] = args;
          const worker = yield* Worker;

          // Add the workflow binding to the Worker metadata
          yield* worker.bind`${name}`({
            bindings: [
              {
                type: "workflow",
                name,
                workflowName: name,
                className: name,
              },
            ],
          });

          // Create the Workflow API resource (putWorkflow / deleteWorkflow)
          yield* WorkflowResource(name, {
            workflowName: name,
            className: name,
            scriptName: worker.workerName,
          });

          const services =
            yield* Effect.context<Effect.Services<typeof impl>>();

          const binding = yield* Effect.all([
            WorkerEnvironment,
            ALCHEMY_PHASE,
          ]).pipe(
            Effect.flatMap(([env, phase]) => {
              if (env === undefined || phase === "plan") {
                return Effect.succeed(undefined as any);
              }
              const wf = env[name];
              if (!wf) {
                return Effect.die(
                  new Error(`Workflow '${name}' not found in env`),
                );
              }
              return Effect.succeed(wf);
            }),
          );

          const self: WorkflowHandle<any, any> = {
            Type: WorkflowTypeId,
            name,
            create: (input?: unknown) =>
              Effect.tryPromise(() =>
                binding.create(toCreateOptions(input)),
              ).pipe(Effect.map(wrapInstance), Effect.orDie),
            createBatch: (batch: WorkflowInstanceCreateOptions<any>[]) =>
              Effect.tryPromise(
                () => binding.createBatch(batch) as Promise<any[]>,
              ).pipe(
                Effect.map((instances: any[]) => instances.map(wrapInstance)),
                Effect.orDie,
              ),
            get: (instanceId: string) =>
              Effect.tryPromise(() => binding.get(instanceId)).pipe(
                Effect.map(wrapInstance),
                Effect.orDie,
              ),
          };

          const fn = yield* impl.pipe(
            Effect.provideService(WorkflowScope, self as any),
          );

          yield* worker.export(name, {
            kind: "workflow",
            make: (env: unknown) =>
              Effect.succeed(((input: unknown) =>
                fn(input).pipe(
                  Effect.provideService(
                    WorkerEnvironment,
                    env as Record<string, any>,
                  ),
                )) as WorkflowImpl<any, any>).pipe(
                Effect.provideContext(services),
              ),
          } satisfies WorkflowExport);

          return self;
        }),
      )) as any);

// ---------------------------------------------------------------------------
// WorkflowResource -- manages the Cloudflare Workflows API lifecycle
// ---------------------------------------------------------------------------

export interface WorkflowResourceProps {
  workflowName: string;
  className: string;
  scriptName: string;
}

export interface WorkflowResourceAttrs {
  workflowId: string;
  workflowName: string;
  className: string;
  scriptName: string;
  accountId: string;
}

const WorkflowResourceTypeId = "Cloudflare.Workflow";

export interface WorkflowResource extends Resource<
  typeof WorkflowResourceTypeId,
  WorkflowResourceProps,
  WorkflowResourceAttrs
> {}

export const WorkflowResource = Resource<WorkflowResource>(
  WorkflowResourceTypeId,
);

export const WorkflowProvider = () =>
  Provider.effect(
    WorkflowResource,
    Effect.gen(function* () {
      const ctx = yield* AlchemyContext;

      return WorkflowResource.Provider.of({
        // The `workflowId` is no longer marked as stable because if you start in dev mode, the ID will change on first deploy.
        stables: ["accountId"],
        diff: Effect.fnUntraced(function* ({ output }) {
          // If the workflowId starts with "dev:", and we're not in dev mode, trigger an update so the workflow is created.
          if (output?.workflowId.startsWith("dev:") && !ctx.dev) {
            return { action: "update" };
          }
        }),
        reconcile: Effect.fnUntraced(function* ({ news, output }) {
          const { accountId } = yield* yield* CloudflareEnvironment;
          const acct = output?.accountId ?? accountId;
          yield* Effect.logInfo(
            `Cloudflare Workflow reconcile: ${news.workflowName}`,
          );
          if (ctx.dev) {
            return {
              workflowId: output?.workflowId ?? `dev:${crypto.randomUUID()}`,
              accountId,
              workflowName: news.workflowName,
              className: news.className,
              scriptName: news.scriptName,
            };
          }
          // Cloudflare's `putWorkflow` is a true PUT-as-upsert: identical
          // payloads converge to the same state and a missing workflow is
          // created on the spot. There is no separate observe step needed
          // — the API is naturally reconciler-shaped.
          const result = yield* workflows.putWorkflow({
            accountId: acct,
            workflowName: news.workflowName,
            className: news.className,
            scriptName: news.scriptName,
          });
          return {
            workflowId: result.id,
            workflowName: result.name,
            className: result.className,
            scriptName: result.scriptName,
            accountId: acct,
          };
        }),
        delete: Effect.fnUntraced(function* ({ output }) {
          yield* Effect.logInfo(
            `Cloudflare Workflow delete: ${output.workflowName}`,
          );
          yield* workflows
            .deleteWorkflow({
              accountId: output.accountId,
              workflowName: output.workflowName,
            })
            .pipe(Effect.catchTag("WorkflowNotFound", () => Effect.void));
        }),
      });
    }),
  );

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const wrapInstance = <Result>(raw: any): WorkflowInstance<Result> => ({
  id: raw.id,
  status: () =>
    Effect.tryPromise(() => raw.status()).pipe(
      Effect.map((s: any) => ({
        status: s.status as string,
        output: s.output as Result,
        error: s.error,
        rollback: s.rollback,
      })),
      Effect.orDie,
    ),
  pause: () => Effect.tryPromise(() => raw.pause()).pipe(Effect.orDie),
  resume: () => Effect.tryPromise(() => raw.resume()).pipe(Effect.orDie),
  restart: (options?: WorkflowInstanceRestartOptions) =>
    Effect.tryPromise(() => raw.restart(options)).pipe(Effect.orDie),
  terminate: () => Effect.tryPromise(() => raw.terminate()).pipe(Effect.orDie),
  sendEvent: <Event = unknown>(event: WorkflowInstanceEvent<Event>) =>
    Effect.tryPromise(() => raw.sendEvent(event)).pipe(Effect.orDie),
});

const toCreateOptions = <Input>(
  input?: Input | WorkflowInstanceCreateOptions<Input>,
): WorkflowInstanceCreateOptions<Input> | undefined => {
  if (input === undefined) return undefined;
  if (isCreateOptions(input)) return input;
  return { params: input };
};

const isCreateOptions = <Input>(
  value: Input | WorkflowInstanceCreateOptions<Input>,
): value is WorkflowInstanceCreateOptions<Input> =>
  typeof value === "object" &&
  value !== null &&
  ("params" in value || "retention" in value || "id" in value);
