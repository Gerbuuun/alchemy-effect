import * as Alchemy from "@/index.ts";
import * as Cloudflare from "@/Cloudflare";
import * as Test from "@/Test/Vitest";
import { expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { MinimumLogLevel } from "effect/References";
import * as Schedule from "effect/Schedule";
import * as HttpClient from "effect/unstable/http/HttpClient";
import WorkflowTestWorker from "./fixtures/workflow-worker.ts";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
});

const logLevel = Effect.provideService(
  MinimumLogLevel,
  process.env.DEBUG ? "Debug" : "Info",
);

const Stack = Alchemy.Stack(
  "WorkflowBindingStack",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const worker = yield* WorkflowTestWorker;
    return {
      url: worker.url.as<string>(),
    };
  }),
);

const stack = beforeAll(deploy(Stack));
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

type WorkflowStatus = {
  status: string;
  output?: {
    greeting: string;
    envBindingCount: number;
    workflowName: string;
    stepAttempt: number;
    instanceId: string;
  };
  error?: { message?: string } | null;
  rollback?: {
    outcome: "complete" | "failed";
    error: { message?: string } | null;
  } | null;
};

const isTerminal = (status: WorkflowStatus) =>
  status.status === "complete" ||
  status.status === "errored" ||
  status.status === "terminated";

const waitForStatus = (
  client: HttpClient.HttpClient,
  url: string,
  id: string,
) =>
  client.get(`${url}/workflow/status/${id}`).pipe(
    Effect.flatMap((res) => res.json as Effect.Effect<unknown>),
    Effect.map((status) => status as WorkflowStatus),
    Effect.repeat({
      schedule: Schedule.spaced("2 seconds"),
      until: isTerminal,
      times: 30,
    }),
  );

test(
  "deployed worker can run a workflow to completion",
  Effect.gen(function* () {
    const out = yield* stack;
    const url = out.url;
    expect(url).toBeTypeOf("string");

    const client = yield* HttpClient.HttpClient;

    // Cloudflare's edge takes a few seconds to start serving a fresh
    // workers.dev URL, so retry until it returns 200 (a fresh URL also
    // returns 404 transiently, which is not an HTTP error so Effect.retry
    // does not catch it unless we explicitly fail on non-200).
    const startRes = yield* client.post(`${url}/workflow/start/world`).pipe(
      Effect.flatMap((res) =>
        res.status === 200
          ? Effect.succeed(res)
          : Effect.fail(new Error(`Worker not ready: ${res.status}`)),
      ),
      Effect.retry({
        schedule: Schedule.exponential("500 millis"),
        times: 15,
      }),
    );
    expect(startRes.status).toBe(200);
    const { instanceId } = (yield* startRes.json) as { instanceId: string };
    expect(instanceId).toBeTypeOf("string");

    const lastStatus = yield* waitForStatus(client, url, instanceId);

    expect(lastStatus).toBeDefined();
    expect(lastStatus.status).toBe("complete");
    expect(lastStatus.error).toBeFalsy();
    expect(lastStatus.output?.greeting).toBe("Hello, world!");
    expect(lastStatus.output?.workflowName).toBe("TestWorkflow");
    expect(lastStatus.output?.stepAttempt).toBe(1);
    expect(lastStatus.output?.instanceId).toBe(instanceId);
    expect(lastStatus.rollback).toBeNull();
    // The body yields `WorkerEnvironment` — if the regression from PR #71 ever
    // returns, the body dies on the first yield and `output` is undefined.
    expect(lastStatus.output?.envBindingCount).toBeGreaterThan(0);
  }).pipe(logLevel),
  { timeout: 180_000 },
);

test(
  "workflow can wait for and receive external events",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const client = yield* HttpClient.HttpClient;

    const startRes = yield* client.post(`${url}/workflow/wait/world`).pipe(
      Effect.flatMap((res) =>
        res.status === 200
          ? Effect.succeed(res)
          : Effect.fail(new Error(`Worker not ready: ${res.status}`)),
      ),
      Effect.retry({
        schedule: Schedule.exponential("500 millis"),
        times: 15,
      }),
    );
    const { instanceId } = (yield* startRes.json) as { instanceId: string };

    const sendRes = yield* client.post(
      `${url}/workflow/send/${instanceId}/external-ok`,
    );
    expect(sendRes.status).toBe(200);

    const lastStatus = yield* waitForStatus(client, url, instanceId);
    expect(lastStatus.status).toBe("complete");
    expect(lastStatus.error).toBeFalsy();
    expect(lastStatus.output?.greeting).toBe("external-ok");
    expect(lastStatus.output?.instanceId).toBe(instanceId);
  }).pipe(logLevel),
  { timeout: 180_000 },
);
