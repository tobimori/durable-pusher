import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { and, eq, gt, lte } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import migrations from "../db/migrations.ts";
import { userGateways, userMetadata } from "../db/schema.ts";
import { mapActorError } from "../pusher/protocol.ts";
import { UserShard, type UserShardApi } from "./contracts.ts";
import { UserActorDependencies } from "./dependencies.ts";

export { UserShard } from "./contracts.ts";

export const UserShardLive = UserShard.make(
  Effect.gen(function* () {
    const { connections: connectionShards } = yield* UserActorDependencies;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      const ensureMetadata = Effect.fn("UserShard.ensureMetadata")(function* (
        placement: ApplicationPlacement,
        userId: string,
      ) {
        yield* db
          .insert(userMetadata)
          .values({
            appId: placement.appId,
            jurisdiction: Option.getOrNull(placement.jurisdiction),
            locationHint: Option.getOrNull(placement.locationHint),
            singleton: 1,
            userId,
          })
          .onConflictDoNothing();
        const [row] = yield* db
          .select({
            appId: userMetadata.appId,
            jurisdiction: userMetadata.jurisdiction,
            locationHint: userMetadata.locationHint,
            userId: userMetadata.userId,
          })
          .from(userMetadata)
          .where(eq(userMetadata.singleton, 1))
          .limit(1);
        const metadata = Option.fromNullishOr(row);
        if (Option.isNone(metadata)) {
          return yield* mapActorError("UserShard", "metadata")("User shard metadata is missing");
        }
        if (
          metadata.value.appId !== placement.appId ||
          metadata.value.userId !== userId ||
          !Equal.equals(
            Option.fromNullishOr(metadata.value.jurisdiction),
            placement.jurisdiction,
          ) ||
          !Equal.equals(Option.fromNullishOr(metadata.value.locationHint), placement.locationHint)
        ) {
          return yield* mapActorError("UserShard", "metadata")("User shard identity mismatch");
        }
      });

      const setGateway = Effect.fn("UserShard.setGateway")(
        function* (
          placement: ApplicationPlacementEncoded,
          userId: string,
          gatewayName: string,
          connectionCount: number,
          generation: number,
        ) {
          const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
          yield* ensureMetadata(decodedPlacement, userId);
          if (connectionCount === 0) {
            yield* db
              .delete(userGateways)
              .where(
                and(
                  eq(userGateways.gatewayName, gatewayName),
                  lte(userGateways.generation, generation),
                ),
              );
            return;
          }
          yield* db
            .insert(userGateways)
            .values({ connectionCount, gatewayName, generation })
            .onConflictDoUpdate({
              target: userGateways.gatewayName,
              set: { connectionCount, generation },
              setWhere: lte(userGateways.generation, generation),
            });
        },
        Effect.mapError(mapActorError("UserShard", "setGateway")),
      );

      const terminate = Effect.fn("UserShard.terminate")(
        function* (placement: ApplicationPlacementEncoded, userId: string) {
          const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
          yield* ensureMetadata(decodedPlacement, userId).pipe(
            Effect.mapError(mapActorError("UserShard", "terminate")),
          );
          const gateways = yield* db
            .select({ gatewayName: userGateways.gatewayName })
            .from(userGateways)
            .where(gt(userGateways.connectionCount, 0))
            .orderBy(userGateways.gatewayName)
            .pipe(Effect.mapError(mapActorError("UserShard", "terminate")));

          const terminated = yield* Effect.forEach(
            gateways,
            Effect.fn("UserShard.terminate.gateway")(function* (gateway) {
              const result = yield* Effect.gen(function* () {
                const connection = yield* connectionShards.getByName(
                  gateway.gatewayName,
                  decodedPlacement,
                );
                return yield* connection.terminateUser(decodedPlacement.appId, userId);
              }).pipe(Effect.mapError(mapActorError("UserShard", "terminate")), Effect.result);
              if (Result.isFailure(result)) {
                yield* Effect.logWarning("User gateway termination failed").pipe(
                  Effect.annotateLogs({
                    actor: result.failure.actor,
                    gatewayName: gateway.gatewayName,
                    message: result.failure.message,
                    operation: result.failure.operation,
                    reason: "termination-failed-retained",
                    userId,
                  }),
                );
              } else {
                yield* db
                  .delete(userGateways)
                  .where(eq(userGateways.gatewayName, gateway.gatewayName))
                  .pipe(Effect.mapError(mapActorError("UserShard", "terminate")));
              }
              return result;
            }),
            { concurrency: 8 },
          );

          let total = 0;
          const failures = [];
          for (const result of terminated) {
            if (Result.isFailure(result)) {
              failures.push(result.failure);
            } else {
              total += result.success;
            }
          }
          const failure = Option.fromNullishOr(failures[0]);
          if (Option.isSome(failure)) {
            return yield* failure.value;
          }
          return total;
        },
        Effect.mapError(mapActorError("UserShard", "terminate")),
      );

      const api = { setGateway, terminate } satisfies UserShardApi;
      return api;
    });
  }),
);
