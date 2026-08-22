import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { eq, sql } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import migrations from "../db/migrations.ts";
import { connectionShardCatalog } from "../db/schema.ts";
import { ActorError, mapActorError } from "../pusher/protocol.ts";
import { ConnectionShardCatalog, type ConnectionShardCatalogApi } from "./contracts.ts";

export { ConnectionShardCatalog };

type CatalogRow = typeof connectionShardCatalog.$inferSelect;

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "ConnectionShardCatalog", message, operation });

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("ConnectionShardCatalog", operation));

export const ConnectionShardCatalogLive = ConnectionShardCatalog.make(
  Effect.succeed(
    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });
      const catalogLock = yield* Semaphore.make(1);
      let cachedCatalog: CatalogRow | undefined;

      const ensure = Effect.fn("ConnectionShardCatalog.ensure")(function* (
        placement: ApplicationPlacement,
      ) {
        if (cachedCatalog === undefined) {
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
          if (row === undefined) {
            return yield* actorError("ensure", "Connection shard catalog is missing");
          }
          cachedCatalog = row;
        }
        if (
          cachedCatalog.appId !== placement.appId ||
          !Equal.equals(Option.fromNullishOr(cachedCatalog.jurisdiction), placement.jurisdiction) ||
          !Equal.equals(Option.fromNullishOr(cachedCatalog.locationHint), placement.locationHint)
        ) {
          return yield* actorError("ensure", "Connection shard catalog identity mismatch");
        }
        return cachedCatalog;
      });

      const shardCountUnlocked = Effect.fn("ConnectionShardCatalog.shardCount")(function* (
        placement: ApplicationPlacementEncoded,
      ) {
        return (yield* ensure(yield* Schema.decodeEffect(ApplicationPlacement)(placement)))
          .shardCount;
      }, operationError("shardCount"));

      const routeUnlocked = Effect.fn("ConnectionShardCatalog.route")(function* (
        placement: ApplicationPlacementEncoded,
        generation: number,
      ) {
        const current = yield* ensure(yield* Schema.decodeEffect(ApplicationPlacement)(placement));
        if (generation <= current.disabledGeneration) {
          return yield* actorError("route", "Application generation is fenced");
        }
        return current.shardCount;
      }, operationError("route"));

      const fenceUnlocked = Effect.fn("ConnectionShardCatalog.fence")(function* (
        placement: ApplicationPlacementEncoded,
        generation: number,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        const current = yield* ensure(decoded);
        const disabledGeneration = Math.max(current.disabledGeneration, generation);
        if (disabledGeneration !== current.disabledGeneration) {
          yield* db
            .update(connectionShardCatalog)
            .set({ disabledGeneration })
            .where(eq(connectionShardCatalog.singleton, 1));
          cachedCatalog = { ...current, disabledGeneration };
        }
        return current.shardCount;
      }, operationError("fence"));

      const expandUnlocked = Effect.fn("ConnectionShardCatalog.expand")(function* (
        placement: ApplicationPlacementEncoded,
        generation: number,
        expectedShardCount: number,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        const current = yield* ensure(decoded);
        if (generation <= current.disabledGeneration) {
          return yield* actorError("expand", "Application generation is fenced");
        }
        if (current.shardCount !== expectedShardCount) {
          return current.shardCount;
        }
        const [updated] = yield* db
          .update(connectionShardCatalog)
          .set({ shardCount: sql`${connectionShardCatalog.shardCount} + 1` })
          .where(
            sql`${connectionShardCatalog.singleton} = 1 AND ${connectionShardCatalog.shardCount} = ${expectedShardCount} AND ${connectionShardCatalog.disabledGeneration} < ${generation}`,
          )
          .returning({ shardCount: connectionShardCatalog.shardCount });
        if (updated === undefined) {
          cachedCatalog = undefined;
          return yield* routeUnlocked(placement, generation);
        }
        cachedCatalog = { ...current, shardCount: updated.shardCount };
        return updated.shardCount;
      }, operationError("expand"));

      const api = {
        expand: (
          placement: ApplicationPlacementEncoded,
          generation: number,
          expectedShardCount: number,
        ) =>
          expandUnlocked(placement, generation, expectedShardCount).pipe(
            Semaphore.withPermit(catalogLock),
          ),
        fence: (placement: ApplicationPlacementEncoded, generation: number) =>
          fenceUnlocked(placement, generation).pipe(Semaphore.withPermit(catalogLock)),
        route: (placement: ApplicationPlacementEncoded, generation: number) =>
          routeUnlocked(placement, generation).pipe(Semaphore.withPermit(catalogLock)),
        shardCount: (placement: ApplicationPlacementEncoded) =>
          shardCountUnlocked(placement).pipe(Semaphore.withPermit(catalogLock)),
      } satisfies ConnectionShardCatalogApi;
      return api;
    }),
  ),
);
