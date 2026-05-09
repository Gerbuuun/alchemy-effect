import * as alerting from "@distilled.cloud/cloudflare/alerting";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import { isResolved } from "../../Diff.ts";
import * as Provider from "../../Provider.ts";
import { Resource } from "../../Resource.ts";
import { CloudflareEnvironment } from "../CloudflareEnvironment.ts";
import type { Providers } from "../Providers.ts";

export type WebhookType =
  | "datadog"
  | "discord"
  | "feishu"
  | "gchat"
  | "generic"
  | "opsgenie"
  | "slack"
  | "splunk";

export type WebhookProps = {
  /**
   * Name of the webhook destination. Cloudflare includes this name in
   * notification payloads sent to the webhook.
   */
  name: string;
  /**
   * POST endpoint Cloudflare calls when dispatching a notification.
   */
  url: string;
  /**
   * Optional secret sent in the `cf-webhook-auth` header for generic
   * webhook notifications or formatted for supported destination types.
   * Cloudflare does not return this value from read/list APIs.
   */
  secret?: Redacted.Redacted<string>;
};

export type Webhook = Resource<
  "Cloudflare.Webhook",
  WebhookProps,
  {
    webhookId: string;
    accountId: string;
    name: string;
    url: string;
    type: WebhookType | undefined;
    createdAt: string | undefined;
    lastFailure: string | undefined;
    lastSuccess: string | undefined;
  },
  never,
  Providers
>;

/**
 * A Cloudflare alerting webhook destination.
 *
 * Webhook destinations receive Cloudflare alert notifications over HTTP.
 * Create a destination once, then reference its `webhookId` from alert
 * notification policies.
 *
 * @section Creating a Webhook
 * @example Basic webhook
 * ```typescript
 * const webhook = yield* Cloudflare.Webhook("Alerts", {
 *   name: "Slack Alerts",
 *   url: "https://hooks.slack.com/services/...",
 * });
 * ```
 *
 * @example Webhook with secret
 * ```typescript
 * import * as Redacted from "effect/Redacted";
 *
 * const webhook = yield* Cloudflare.Webhook("GenericAlerts", {
 *   name: "Generic Alerts",
 *   url: "https://example.com/cloudflare/alerts",
 *   secret: Redacted.make(process.env.CLOUDFLARE_WEBHOOK_SECRET!),
 * });
 * ```
 */
export const Webhook = Resource<Webhook>("Cloudflare.Webhook");

type WebhookResponse =
  | alerting.GetDestinationWebhookResponse
  | NonNullable<alerting.ListDestinationWebhooksResponse["result"][number]>;

const unwrapSecret = (
  secret: Redacted.Redacted<string> | undefined,
): string | undefined => (secret ? Redacted.value(secret) : undefined);

const isNotFoundError = (error: unknown): boolean => {
  if (!error || typeof error !== "object") return false;
  const tag = (error as { _tag?: unknown })._tag;
  const status = (error as { status?: unknown }).status;
  return tag === "NotFound" || tag === "WebhookNotFound" || status === 404;
};

export const WebhookProvider = () =>
  Provider.effect(
    Webhook,
    Effect.gen(function* () {
      const { accountId } = yield* CloudflareEnvironment;
      const createWebhook = yield* alerting.createDestinationWebhook;
      const getWebhook = yield* alerting.getDestinationWebhook;
      const updateWebhook = yield* alerting.updateDestinationWebhook;
      const deleteWebhook = yield* alerting.deleteDestinationWebhook;
      const listWebhooks = alerting.listDestinationWebhooks;

      const findWebhookByName = (accountId: string, name: string) =>
        listWebhooks.items({ accountId }).pipe(
          Stream.filter((webhook) => webhook.name === name),
          Stream.runHead,
          Effect.map(Option.getOrUndefined),
        );

      const mapWebhook = (
        webhook: WebhookResponse,
        accountId: string,
        fallback: Pick<Webhook["Attributes"], "webhookId" | "name" | "url">,
      ): Webhook["Attributes"] => ({
        webhookId: webhook.id ?? fallback.webhookId,
        accountId,
        name: webhook.name ?? fallback.name,
        url: webhook.url ?? fallback.url,
        type: webhook.type ?? undefined,
        createdAt: webhook.createdAt ?? undefined,
        lastFailure: webhook.lastFailure ?? undefined,
        lastSuccess: webhook.lastSuccess ?? undefined,
      });

      const observeById = (
        accountId: string,
        webhookId: string | undefined,
      ) => {
        if (!webhookId) {
          return Effect.succeed(undefined);
        }
        return getWebhook({ accountId, webhookId }).pipe(
          Effect.catchIf(isNotFoundError, () => Effect.succeed(undefined)),
        );
      };

      return {
        stables: ["webhookId", "accountId"],
        diff: Effect.fn(function* ({ olds = {}, news, output }) {
          if (!isResolved(news)) return undefined;
          if ((output?.accountId ?? accountId) !== accountId) {
            return { action: "replace" } as const;
          }
          if (
            (output?.name ?? olds.name) !== news.name ||
            (output?.url ?? olds.url) !== news.url ||
            unwrapSecret(olds.secret) !== unwrapSecret(news.secret)
          ) {
            return { action: "update" } as const;
          }
        }),
        reconcile: Effect.fn(function* ({ news, olds, output }) {
          const acct = output?.accountId ?? accountId;

          // Observe — prefer the cached Cloudflare id, then fall back to
          // a name scan so state-loss adoption and interrupted creates
          // converge on the existing destination.
          let observed = yield* observeById(acct, output?.webhookId);
          if (!observed) {
            observed = yield* findWebhookByName(acct, news.name);
          }

          // Ensure — create if missing. Cloudflare permits multiple
          // destinations with different ids, so an existing name match
          // is intentionally adopted before this point.
          let webhookId = observed?.id ?? undefined;
          if (!webhookId) {
            const created = yield* createWebhook({
              accountId: acct,
              name: news.name,
              url: news.url,
              secret: unwrapSecret(news.secret),
            });
            if (!created.id) {
              return yield* Effect.die(
                "Cloudflare webhook create succeeded without returning an id.",
              );
            }
            webhookId = created.id;
          }

          const secretChanged =
            !olds || unwrapSecret(olds.secret) !== unwrapSecret(news.secret);
          const needsUpdate =
            !observed ||
            observed.name !== news.name ||
            observed.url !== news.url ||
            secretChanged;

          if (needsUpdate) {
            yield* updateWebhook({
              accountId: acct,
              webhookId,
              name: news.name,
              url: news.url,
              secret: unwrapSecret(news.secret),
            });
          }

          const synced = yield* getWebhook({ accountId: acct, webhookId });
          return mapWebhook(synced, acct, {
            webhookId,
            name: news.name,
            url: news.url,
          });
        }),
        delete: Effect.fn(function* ({ output }) {
          yield* deleteWebhook({
            accountId: output.accountId,
            webhookId: output.webhookId,
          }).pipe(Effect.catchIf(isNotFoundError, () => Effect.void));
        }),
        read: Effect.fn(function* ({ olds, output }) {
          const acct = output?.accountId ?? accountId;
          const byId = yield* observeById(acct, output?.webhookId);
          if (byId) {
            return mapWebhook(byId, acct, {
              webhookId: output!.webhookId,
              name: output?.name ?? olds?.name ?? "",
              url: output?.url ?? olds?.url ?? "",
            });
          }
          if (!olds?.name) {
            return undefined;
          }
          const byName = yield* findWebhookByName(acct, olds.name);
          if (!byName?.id) {
            return undefined;
          }
          return mapWebhook(byName, acct, {
            webhookId: byName.id,
            name: olds.name,
            url: olds.url,
          });
        }),
      };
    }),
  );
