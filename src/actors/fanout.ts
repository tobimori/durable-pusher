import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { asc, eq, gt, inArray, sql, sum } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import migrations from "../db/migrations.ts";
import { fanoutGateways, fanoutMetadata, fanoutState } from "../db/schema.ts";
import { ActorError, mapActorError, type Delivery } from "../pusher/protocol.ts";
import { channelShardName } from "../sharding.ts";
import { FanoutShard } from "./contracts.ts";
import { FanoutActorDependencies } from "./dependencies.ts";

export { FanoutShard };

const isActorError = Schema.is(ActorError);

const toActorError = (
  operation: string,
  error: ActorError | { readonly message?: string },
): ActorError => (isActorError(error) ? error : mapActorError("FanoutShard", operation)(error));

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "FanoutShard", message, operation });

export const FanoutShardLive = FanoutShard.make(
  Effect.gen(function* () {
    const {
      channels: channelShards,
      connections: connectionShards,
    } = yield* FanoutActorDependencies;
    const state = yield* Cloudflare.DurableObjectState;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      const metadata = Effect.fn("FanoutShard.metadata")(function* (operation: string) {
        const [value] = yield* db
          .select({
            appId: fanoutMetadata.appId,
            branchName: fanoutMetadata.branchName,
            channel: fanoutMetadata.channel,
          })
          .from(fanoutMetadata)
          .where(eq(fanoutMetadata.singleton, 1))
          .limit(1);
        if (value === undefined) {
          return yield* actorError(operation, "Fanout shard metadata is missing");
        }
        return value;
      });

      const summary = Effect.fn("FanoutShard.summary")(function* () {
        const [count] = yield* db
          .select({
            subscriptionCount: sum(fanoutGateways.subscriptionCount).mapWith(Number),
          })
          .from(fanoutGateways);
        const [current] = yield* db
          .select({ generation: fanoutState.generation })
          .from(fanoutState)
          .where(eq(fanoutState.singleton, 1))
          .limit(1);
        return {
          generation: current?.generation ?? 0,
          subscriptionCount: count?.subscriptionCount ?? 0,
        };
      });

      const syncBranch = Effect.fn("FanoutShard.syncBranch")(function* (
        identity: {
          readonly appId: string;
          readonly branchName: string;
          readonly channel: string;
        },
        current: {
          readonly generation: number;
          readonly subscriptionCount: number;
        },
      ) {
        return yield* channelShards
          .getByName(channelShardName(identity.appId, identity.channel))
          .setBranch(
            identity.appId,
            identity.channel,
            identity.branchName,
            current.subscriptionCount,
            current.generation,
          );
      });

      const setGateway = Effect.fn("FanoutShard.setGateway")(
        function* (
          appId: string,
          channel: string,
          branchName: string,
          gatewayName: string,
          subscriptionCount: number,
          gatewayGeneration: number,
        ) {
          const current = yield* db.transaction(
            Effect.fn("FanoutShard.setGateway.transaction")(function* (tx) {
              const [identity] = yield* tx
                .select({
                  appId: fanoutMetadata.appId,
                  branchName: fanoutMetadata.branchName,
                  channel: fanoutMetadata.channel,
                })
                .from(fanoutMetadata)
                .where(eq(fanoutMetadata.singleton, 1))
                .limit(1);

              if (identity === undefined) {
                yield* tx.insert(fanoutMetadata).values({
                  appId,
                  branchName,
                  channel,
                  singleton: 1,
                });
              } else if (
                identity.appId !== appId ||
                identity.channel !== channel ||
                (identity.branchName !== "" && identity.branchName !== branchName)
              ) {
                return yield* actorError("setGateway", "Fanout shard identity mismatch");
              } else if (identity.branchName === "") {
                yield* tx
                  .update(fanoutMetadata)
                  .set({ branchName })
                  .where(eq(fanoutMetadata.singleton, 1));
              }

              const [gateway] = yield* tx
                .select({ generation: fanoutGateways.generation })
                .from(fanoutGateways)
                .where(eq(fanoutGateways.gatewayName, gatewayName))
                .limit(1);
              const accepted = gateway === undefined || gateway.generation <= gatewayGeneration;

              if (accepted) {
                if (subscriptionCount === 0) {
                  yield* tx
                    .delete(fanoutGateways)
                    .where(eq(fanoutGateways.gatewayName, gatewayName));
                } else {
                  yield* tx
                    .insert(fanoutGateways)
                    .values({
                      gatewayName,
                      generation: gatewayGeneration,
                      subscriptionCount,
                    })
                    .onConflictDoUpdate({
                      target: fanoutGateways.gatewayName,
                      set: {
                        generation: gatewayGeneration,
                        subscriptionCount,
                      },
                    });
                }
                yield* tx
                  .insert(fanoutState)
                  .values({ generation: 1, singleton: 1 })
                  .onConflictDoUpdate({
                    target: fanoutState.singleton,
                    set: {
                      generation: sql`${fanoutState.generation} + 1`,
                    },
                  });
              }

              const [count] = yield* tx
                .select({
                  subscriptionCount: sum(fanoutGateways.subscriptionCount).mapWith(Number),
                })
                .from(fanoutGateways);
              const [branch] = yield* tx
                .select({ generation: fanoutState.generation })
                .from(fanoutState)
                .where(eq(fanoutState.singleton, 1))
                .limit(1);
              return {
                generation: branch?.generation ?? 0,
                subscriptionCount: count?.subscriptionCount ?? 0,
              };
            }),
          );

          return yield* syncBranch({ appId, branchName, channel }, current);
        },
        Effect.mapError((error) => toActorError("setGateway", error)),
      );

      const deliver = Effect.fn("FanoutShard.deliver")(
        function* (delivery: Delivery) {
          const identity = yield* metadata("deliver");
          if (identity.channel !== delivery.channel) {
            return yield* actorError("deliver", "Fanout delivery channel mismatch");
          }

          const gateways = yield* db
            .select({ gatewayName: fanoutGateways.gatewayName })
            .from(fanoutGateways)
            .where(gt(fanoutGateways.subscriptionCount, 0))
            .orderBy(asc(fanoutGateways.gatewayName));
          const results = yield* Effect.forEach(
            gateways,
            Effect.fn("FanoutShard.deliver.gateway")(function* (gateway) {
              const result = yield* connectionShards
                .getByName(gateway.gatewayName)
                .deliver(delivery)
                .pipe(Effect.result);
              if (Result.isFailure(result)) {
                yield* Effect.logWarning("Pruning failed fanout gateway").pipe(
                  Effect.annotateLogs({
                    actor: result.failure.actor,
                    error: result.failure.message,
                    failedOperation: result.failure.operation,
                    gatewayName: gateway.gatewayName,
                    operation: "deliver",
                    reason: "delivery-failed",
                  }),
                );
                return gateway.gatewayName;
              }
              if (result.success === 0) {
                yield* Effect.logInfo("Pruning empty fanout gateway").pipe(
                  Effect.annotateLogs({
                    actor: "FanoutShard",
                    gatewayName: gateway.gatewayName,
                    operation: "deliver",
                    reason: "zero-subscriptions",
                  }),
                );
                return gateway.gatewayName;
              }
              return undefined;
            }),
            { concurrency: 8 },
          );
          const gatewayNames = results.filter(
            (gatewayName): gatewayName is string => gatewayName !== undefined,
          );
          if (gatewayNames.length === 0) {
            return;
          }

          const changed = yield* db.transaction(
            Effect.fn("FanoutShard.deliver.transaction")(function* (tx) {
              const deleted = yield* tx
                .delete(fanoutGateways)
                .where(inArray(fanoutGateways.gatewayName, gatewayNames))
                .returning({ gatewayName: fanoutGateways.gatewayName });
              if (deleted.length === 0) {
                return false;
              }
              yield* tx
                .insert(fanoutState)
                .values({ generation: 1, singleton: 1 })
                .onConflictDoUpdate({
                  target: fanoutState.singleton,
                  set: { generation: sql`${fanoutState.generation} + 1` },
                });
              return true;
            }),
          );
          if (changed) {
            const now = yield* Clock.currentTimeMillis;
            yield* state.storage.setAlarm(now + 1);
          }
        },
        Effect.mapError((error) => toActorError("deliver", error)),
      );

      const alarm = Effect.fn("FanoutShard.alarm")(
        function* () {
          const identity = yield* metadata("alarm");
          const current = yield* summary();
          yield* syncBranch(identity, current);
        },
        Effect.mapError((error) => toActorError("alarm", error)),
        Effect.catch(
          Effect.fn("FanoutShard.alarm.handleError")(function* (error) {
            yield* Effect.logError("Fanout shard alarm resync failed").pipe(
              Effect.annotateLogs({
                actor: error.actor,
                error: error.message,
                failedOperation: error.operation,
                operation: "alarm",
              }),
            );
          }),
        ),
      );

      return { alarm, deliver, setGateway };
    });
  }),
);
