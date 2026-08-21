import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { and, eq, gt, lte } from "drizzle-orm";
import * as Effect from "effect/Effect";
import migrations from "../db/migrations.ts";
import { userGateways } from "../db/schema.ts";
import { mapActorError } from "../pusher/protocol.ts";
import { UserShard } from "./contracts.ts";
import { UserActorDependencies } from "./dependencies.ts";

export { UserShard } from "./contracts.ts";

export const UserShardLive = UserShard.make(
  Effect.gen(function* () {
    const { connections: connectionShards } = yield* UserActorDependencies;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      return {
        setGateway: Effect.fn("UserShard.setGateway")(
          function* (gatewayName, connectionCount, generation) {
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
        ),
        terminate: Effect.fn("UserShard.terminate")(function* (userId) {
          const gateways = yield* db
            .select({ gatewayName: userGateways.gatewayName })
            .from(userGateways)
            .where(gt(userGateways.connectionCount, 0))
            .orderBy(userGateways.gatewayName)
            .pipe(Effect.mapError(mapActorError("UserShard", "terminate")));

          const terminated = yield* Effect.forEach(
            gateways,
            (gateway) =>
              connectionShards
                .getByName(gateway.gatewayName)
                .terminateUser(userId)
                .pipe(
                  Effect.catch((error) =>
                    Effect.gen(function* () {
                      yield* Effect.logWarning("Pruning stale user gateway").pipe(
                        Effect.annotateLogs({
                          actor: error.actor,
                          gatewayName: gateway.gatewayName,
                          message: error.message,
                          operation: error.operation,
                          userId,
                        }),
                      );
                      yield* db
                        .delete(userGateways)
                        .where(eq(userGateways.gatewayName, gateway.gatewayName))
                        .pipe(Effect.mapError(mapActorError("UserShard", "terminate")));
                      return 0;
                    }),
                  ),
                ),
            { concurrency: 8 },
          );

          yield* db
            .delete(userGateways)
            .pipe(Effect.mapError(mapActorError("UserShard", "terminate")));
          return terminated.reduce((total, count) => total + count, 0);
        }),
      };
    });
  }),
);
