import * as Cloudflare from "@/Cloudflare";
import { CloudflareEnvironment } from "@/Cloudflare/CloudflareEnvironment";
import { State } from "@/State";
import * as Test from "@/Test/Vitest";
import * as alerting from "@distilled.cloud/cloudflare/alerting";
import { expect } from "@effect/vitest";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import { MinimumLogLevel } from "effect/References";
import * as Schedule from "effect/Schedule";

const { test } = Test.make({ providers: Cloudflare.providers() });

const logLevel = Effect.provideService(
  MinimumLogLevel,
  process.env.DEBUG ? "Debug" : "Info",
);

test.provider("create, update, adopt by name, delete", (stack) =>
  Effect.gen(function* () {
    const { accountId } = yield* CloudflareEnvironment;
    const name = `alchemy-test-webhook-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

    yield* stack.destroy();

    const initial = yield* stack.deploy(
      Effect.gen(function* () {
        return yield* Cloudflare.Webhook("Webhook", {
          name,
          url: "https://httpbin.org/post",
        });
      }),
    );

    expect(initial.webhookId).toBeTypeOf("string");
    expect(initial.name).toEqual(name);
    expect(initial.url).toEqual("https://httpbin.org/post");

    const live = yield* alerting.getDestinationWebhook({
      accountId,
      webhookId: initial.webhookId,
    });
    expect(live.name).toEqual(name);
    expect(live.url).toEqual("https://httpbin.org/post");

    const updated = yield* stack.deploy(
      Effect.gen(function* () {
        return yield* Cloudflare.Webhook("Webhook", {
          name,
          url: "https://httpbin.org/anything",
        });
      }),
    );

    expect(updated.webhookId).toEqual(initial.webhookId);
    expect(updated.url).toEqual("https://httpbin.org/anything");

    // Wipe local state while leaving the Cloudflare destination in place.
    // The next deploy must recover it by name instead of creating a duplicate.
    yield* Effect.gen(function* () {
      const state = yield* State;
      yield* state.delete({
        stack: stack.name,
        stage: "test",
        fqn: "Webhook",
      });
    }).pipe(Effect.provide(stack.state));

    const adopted = yield* stack.deploy(
      Effect.gen(function* () {
        return yield* Cloudflare.Webhook("Webhook", {
          name,
          url: "https://httpbin.org/anything",
        });
      }),
    );

    expect(adopted.webhookId).toEqual(initial.webhookId);

    yield* stack.destroy();
    yield* waitForWebhookToBeDeleted(accountId, initial.webhookId);
  }).pipe(logLevel),
);

const waitForWebhookToBeDeleted = Effect.fn(function* (
  accountId: string,
  webhookId: string,
) {
  yield* alerting
    .getDestinationWebhook({
      accountId,
      webhookId,
    })
    .pipe(
      Effect.flatMap(() => Effect.fail(new WebhookStillExists())),
      Effect.retry({
        while: (e): e is WebhookStillExists => e instanceof WebhookStillExists,
        schedule: Schedule.exponential(100),
      }),
      Effect.catchTag("WebhookNotFound", () => Effect.void),
      Effect.catchIf(
        (e) =>
          e._tag === "CloudflareHttpError" &&
          (e as { status: number }).status === 404,
        () => Effect.void,
      ),
    );
});

class WebhookStillExists extends Data.TaggedError("WebhookStillExists") {}
