import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { asc, eq, gt, inArray, sql, sum } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import migrations from "../db/migrations.ts";
import { fanoutGateways, fanoutMetadata, fanoutState } from "../db/schema.ts";
import { ActorError, Delivery, mapActorError, type DeliveryEncoded } from "../pusher/protocol.ts";
import { channelShardName } from "../sharding.ts";
import { FanoutShard, type FanoutShardApi } from "./contracts.ts";
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
    const { channels: channelShards, connections: connectionShards } =
      yield* FanoutActorDependencies;
    const state = yield* Cloudflare.DurableObjectState;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      const metadata = Effect.fn("FanoutShard.metadata")(function* (operation: string) {
        const [row] = yield* db
          .select({
            appId: fanoutMetadata.appId,
            branchName: fanoutMetadata.branchName,
            channel: fanoutMetadata.channel,
            jurisdiction: fanoutMetadata.jurisdiction,
            locationHint: fanoutMetadata.locationHint,
          })
          .from(fanoutMetadata)
          .where(eq(fanoutMetadata.singleton, 1))
          .limit(1);
        const value = Option.fromNullishOr(row);
        if (Option.isNone(value)) {
          return yield* actorError(operation, "Fanout shard metadata is missing");
        }
        return {
          appId: value.value.appId,
          branchName: value.value.branchName,
          channel: value.value.channel,
          jurisdiction: Option.fromNullishOr(value.value.jurisdiction),
          locationHint: Option.fromNullishOr(value.value.locationHint),
        };
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
          readonly jurisdiction: ApplicationPlacement["jurisdiction"];
          readonly locationHint: ApplicationPlacement["locationHint"];
        },
        current: {
          readonly generation: number;
          readonly subscriptionCount: number;
        },
      ) {
        const placement: ApplicationPlacement = {
          appId: identity.appId,
          jurisdiction: identity.jurisdiction,
          locationHint: identity.locationHint,
        };
        const channel = yield* channelShards.getByName(
          channelShardName(identity.appId, identity.channel),
          placement,
        );
        return yield* channel.setBranch(
          yield* Schema.encodeEffect(ApplicationPlacement)(placement),
          identity.channel,
          identity.branchName,
          current.subscriptionCount,
          current.generation,
        );
      });

      const setGateway = Effect.fn("FanoutShard.setGateway")(
        function* (
          placement: ApplicationPlacementEncoded,
          channel: string,
          branchName: string,
          gatewayName: string,
          subscriptionCount: number,
          gatewayGeneration: number,
        ) {
          const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
          const current = yield* db.transaction(
            Effect.fn("FanoutShard.setGateway.transaction")(function* (tx) {
              const [identityRow] = yield* tx
                .select({
                  appId: fanoutMetadata.appId,
                  branchName: fanoutMetadata.branchName,
                  channel: fanoutMetadata.channel,
                  jurisdiction: fanoutMetadata.jurisdiction,
                  locationHint: fanoutMetadata.locationHint,
                })
                .from(fanoutMetadata)
                .where(eq(fanoutMetadata.singleton, 1))
                .limit(1);

              const identity = Option.fromNullishOr(identityRow);
              if (Option.isNone(identity)) {
                yield* tx.insert(fanoutMetadata).values({
                  appId: decodedPlacement.appId,
                  branchName,
                  channel,
                  jurisdiction: Option.getOrNull(decodedPlacement.jurisdiction),
                  locationHint: Option.getOrNull(decodedPlacement.locationHint),
                  singleton: 1,
                });
              } else if (
                identity.value.appId !== decodedPlacement.appId ||
                identity.value.channel !== channel ||
                !Equal.equals(
                  Option.fromNullishOr(identity.value.jurisdiction),
                  decodedPlacement.jurisdiction,
                ) ||
                !Equal.equals(
                  Option.fromNullishOr(identity.value.locationHint),
                  decodedPlacement.locationHint,
                ) ||
                (identity.value.branchName !== "" && identity.value.branchName !== branchName)
              ) {
                return yield* actorError("setGateway", "Fanout shard identity mismatch");
              } else if (identity.value.branchName === "") {
                yield* tx
                  .update(fanoutMetadata)
                  .set({ branchName })
                  .where(eq(fanoutMetadata.singleton, 1));
              }

              const [gatewayRow] = yield* tx
                .select({ generation: fanoutGateways.generation })
                .from(fanoutGateways)
                .where(eq(fanoutGateways.gatewayName, gatewayName))
                .limit(1);
              const gateway = Option.fromNullishOr(gatewayRow);
              const accepted =
                Option.isNone(gateway) || gateway.value.generation <= gatewayGeneration;

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

          return yield* syncBranch({ ...decodedPlacement, branchName, channel }, current);
        },
        Effect.mapError((error) => toActorError("setGateway", error)),
      );

      const deliver = Effect.fn("FanoutShard.deliver")(
        function* (encodedDelivery: DeliveryEncoded) {
          const delivery = yield* Schema.decodeEffect(Delivery)(encodedDelivery);
          const identity = yield* metadata("deliver");
          if (
            identity.appId !== delivery.appId ||
            identity.channel !== delivery.channel ||
            !Equal.equals(identity.jurisdiction, delivery.jurisdiction) ||
            !Equal.equals(identity.locationHint, delivery.locationHint)
          ) {
            return yield* actorError("deliver", "Fanout delivery identity mismatch");
          }
          const placement: ApplicationPlacement = {
            appId: identity.appId,
            jurisdiction: identity.jurisdiction,
            locationHint: identity.locationHint,
          };

          const gateways = yield* db
            .select({ gatewayName: fanoutGateways.gatewayName })
            .from(fanoutGateways)
            .where(gt(fanoutGateways.subscriptionCount, 0))
            .orderBy(asc(fanoutGateways.gatewayName));
          const results = yield* Effect.forEach(
            gateways,
            Effect.fn("FanoutShard.deliver.gateway")(function* (gateway) {
              const result = yield* Effect.gen(function* () {
                const connection = yield* connectionShards.getByName(
                  gateway.gatewayName,
                  placement,
                );
                return yield* connection.deliver(encodedDelivery);
              }).pipe(
                Effect.mapError((error) => toActorError("deliver", error)),
                Effect.result,
              );
              if (Result.isFailure(result)) {
                yield* Effect.logWarning("Fanout gateway delivery failed").pipe(
                  Effect.annotateLogs({
                    actor: result.failure.actor,
                    error: result.failure.message,
                    failedOperation: result.failure.operation,
                    gatewayName: gateway.gatewayName,
                    operation: "deliver",
                    reason: "delivery-failed-retained",
                  }),
                );
              } else if (result.success === 0) {
                yield* Effect.logInfo("Pruning empty fanout gateway").pipe(
                  Effect.annotateLogs({
                    actor: "FanoutShard",
                    gatewayName: gateway.gatewayName,
                    operation: "deliver",
                    reason: "zero-subscriptions",
                  }),
                );
              }
              return { gatewayName: gateway.gatewayName, result };
            }),
            { concurrency: 8 },
          );
          const gatewayNames: string[] = [];
          const failures: ActorError[] = [];
          for (const result of results) {
            if (Result.isFailure(result.result)) {
              failures.push(result.result.failure);
            } else if (result.result.success === 0) {
              gatewayNames.push(result.gatewayName);
            }
          }
          const changed =
            gatewayNames.length === 0
              ? false
              : yield* db.transaction(
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
          const failure = Option.fromNullishOr(failures[0]);
          if (Option.isSome(failure)) {
            return yield* failure.value;
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

      const api = { deliver, setGateway } satisfies FanoutShardApi;
      return { alarm, ...api };
    });
  }),
);
