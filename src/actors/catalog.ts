import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { eq, sql } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import migrations from "../db/migrations.ts";
import { connectionShardCatalog } from "../db/schema.ts";
import { ActorError, mapActorError } from "../pusher/protocol.ts";
import { ConnectionShardCatalog, type ConnectionShardCatalogApi } from "./contracts.ts";

export { ConnectionShardCatalog };

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "ConnectionShardCatalog", message, operation });

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("ConnectionShardCatalog", operation));

export const ConnectionShardCatalogLive = ConnectionShardCatalog.make(
  Effect.succeed(
    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      const ensure = Effect.fn("ConnectionShardCatalog.ensure")(function* (
        placement: ApplicationPlacement,
      ) {
        yield* db
          .insert(connectionShardCatalog)
          .values({
            appId: placement.appId,
            jurisdiction: Option.getOrNull(placement.jurisdiction),
            locationHint: Option.getOrNull(placement.locationHint),
            shardCount: 1,
            singleton: 1,
          })
          .onConflictDoNothing();
        const [row] = yield* db
          .select()
          .from(connectionShardCatalog)
          .where(eq(connectionShardCatalog.singleton, 1))
          .limit(1);
        const current = Option.fromNullishOr(row);
        if (Option.isNone(current)) {
          return yield* actorError("ensure", "Connection shard catalog is missing");
        }
        if (
          current.value.appId !== placement.appId ||
          !Equal.equals(Option.fromNullishOr(current.value.jurisdiction), placement.jurisdiction) ||
          !Equal.equals(Option.fromNullishOr(current.value.locationHint), placement.locationHint)
        ) {
          return yield* actorError("ensure", "Connection shard catalog identity mismatch");
        }
        return current.value.shardCount;
      });

      const shardCount = Effect.fn("ConnectionShardCatalog.shardCount")(function* (
        placement: ApplicationPlacementEncoded,
      ) {
        return yield* ensure(yield* Schema.decodeEffect(ApplicationPlacement)(placement));
      }, operationError("shardCount"));

      const expand = Effect.fn("ConnectionShardCatalog.expand")(function* (
        placement: ApplicationPlacementEncoded,
        expectedShardCount: number,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        const current = yield* ensure(decoded);
        if (current !== expectedShardCount) {
          return current;
        }
        const [updated] = yield* db
          .update(connectionShardCatalog)
          .set({ shardCount: sql`${connectionShardCatalog.shardCount} * 2` })
          .where(
            sql`${connectionShardCatalog.singleton} = 1 AND ${connectionShardCatalog.shardCount} = ${expectedShardCount}`,
          )
          .returning({ shardCount: connectionShardCatalog.shardCount });
        return updated?.shardCount ?? (yield* ensure(decoded));
      }, operationError("expand"));

      const api = { expand, shardCount } satisfies ConnectionShardCatalogApi;
      return api;
    }),
  ),
);
