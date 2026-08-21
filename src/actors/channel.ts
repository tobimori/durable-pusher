import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { and, count, countDistinct, eq, gt, sql } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import migrations from "../db/migrations.ts";
import {
  branchDeliveries,
  cacheEvents,
  channelBranches,
  channelMetadata,
  channelState,
  outboxEvents,
  presenceMembers,
} from "../db/schema.ts";
import {
  ActorError,
  classifyChannel,
  decodeJson,
  Delivery,
  encodeJson,
  mapActorError,
  PresenceJoin,
  type CachedEvent,
  type ChannelInfo,
  type ChannelSnapshot,
  type PresenceJoinEncoded,
  type PresenceMember,
  type PresenceSnapshot,
} from "../pusher/protocol.ts";
import { directoryShardName } from "../sharding.ts";
import { ChannelShard, type ChannelShardApi } from "./contracts.ts";
import { ChannelActorDependencies } from "./dependencies.ts";

export { ChannelShard };

type OutboxEvent = typeof outboxEvents.$inferSelect;

const OUTBOX_RETENTION_MS = 60_000;
const CACHE_RETENTION_MS = 30 * 60_000;
const RETRY_DELAY_MS = 1_000;
const DELIVERY_DEFERRED = "ChannelDeliveryDeferred";

const operationError = (operation: string) =>
  Effect.mapError((error: { readonly message?: string } | string) =>
    mapActorError("ChannelShard", operation)(error),
  );

const failActor = (operation: string, message: string) =>
  Effect.fail(ActorError.make({ actor: "ChannelShard", message, operation }));

export const ChannelShardLive = ChannelShard.make(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const { directories, fanouts } = yield* ChannelActorDependencies;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });
      yield* db
        .insert(channelState)
        .values({ sequence: 0, singleton: 1 })
        .onConflictDoNothing()
        .pipe(Effect.orDie);

      const pumpLock = yield* Ref.make(false);

      const ensureMetadata = Effect.fn("ChannelShard.ensureMetadata")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        yield* db
          .insert(channelMetadata)
          .values({
            appId: placement.appId,
            channel,
            jurisdiction: Option.getOrNull(placement.jurisdiction),
            locationHint: Option.getOrNull(placement.locationHint),
            singleton: 1,
          })
          .onConflictDoNothing();
        const rows = yield* db
          .select({
            appId: channelMetadata.appId,
            channel: channelMetadata.channel,
            jurisdiction: channelMetadata.jurisdiction,
            locationHint: channelMetadata.locationHint,
          })
          .from(channelMetadata)
          .where(eq(channelMetadata.singleton, 1))
          .limit(1);
        const metadata = Option.fromNullishOr(rows[0]);
        if (Option.isNone(metadata)) {
          return yield* failActor("metadata", "Channel shard metadata is missing");
        }
        if (
          metadata.value.appId !== placement.appId ||
          metadata.value.channel !== channel ||
          !Equal.equals(
            Option.fromNullishOr(metadata.value.jurisdiction),
            placement.jurisdiction,
          ) ||
          !Equal.equals(Option.fromNullishOr(metadata.value.locationHint), placement.locationHint)
        ) {
          return yield* failActor("metadata", "Channel shard identity mismatch");
        }
      });

      const getMetadata = Effect.fn("ChannelShard.getMetadata")(function* () {
        const rows = yield* db
          .select({
            appId: channelMetadata.appId,
            channel: channelMetadata.channel,
            jurisdiction: channelMetadata.jurisdiction,
            locationHint: channelMetadata.locationHint,
          })
          .from(channelMetadata)
          .where(eq(channelMetadata.singleton, 1))
          .limit(1);
        const metadata = Option.fromNullishOr(rows[0]);
        if (Option.isNone(metadata)) {
          return yield* failActor("metadata", "Channel shard metadata is missing");
        }
        return {
          appId: metadata.value.appId,
          channel: metadata.value.channel,
          jurisdiction: Option.fromNullishOr(metadata.value.jurisdiction),
          locationHint: Option.fromNullishOr(metadata.value.locationHint),
        };
      });

      const currentSequence = Effect.fn("ChannelShard.currentSequence")(function* () {
        const rows = yield* db
          .select({ sequence: channelState.sequence })
          .from(channelState)
          .where(eq(channelState.singleton, 1))
          .limit(1);
        return rows[0]?.sequence ?? 0;
      });

      const subscriptionCount = Effect.fn("ChannelShard.subscriptionCount")(function* () {
        const rows = yield* db
          .select({ count: sql<number>`coalesce(sum(${channelBranches.subscriptionCount}), 0)` })
          .from(channelBranches);
        return rows[0]?.count ?? 0;
      });

      const userCount = Effect.fn("ChannelShard.userCount")(function* () {
        const rows = yield* db
          .select({ count: countDistinct(presenceMembers.userId) })
          .from(presenceMembers);
        return rows[0]?.count ?? 0;
      });

      const userConnectionCount = Effect.fn("ChannelShard.userConnectionCount")(function* (
        userId: string,
      ) {
        const rows = yield* db
          .select({ count: count() })
          .from(presenceMembers)
          .where(eq(presenceMembers.userId, userId));
        return rows[0]?.count ?? 0;
      });

      const getPresenceMembers = Effect.fn("ChannelShard.getPresenceMembers")(function* () {
        const rows = yield* db
          .select({
            userId: presenceMembers.userId,
            userInfo: sql<string>`min(${presenceMembers.userInfo})`,
          })
          .from(presenceMembers)
          .groupBy(presenceMembers.userId)
          .orderBy(presenceMembers.userId);
        return yield* Effect.forEach(
          rows,
          Effect.fn("ChannelShard.decodePresenceMember")(function* (row) {
            const userInfo = yield* decodeJson(row.userInfo);
            return { userId: row.userId, userInfo } satisfies PresenceMember;
          }),
        );
      });

      const getCachedEvent = Effect.fn("ChannelShard.getCachedEvent")(function* () {
        const rows = yield* db
          .select({
            data: cacheEvents.data,
            event: cacheEvents.event,
            expiresAt: cacheEvents.expiresAt,
            sequence: cacheEvents.sequence,
          })
          .from(cacheEvents)
          .where(eq(cacheEvents.singleton, 1))
          .limit(1);
        const cached = rows[0];
        if (cached === undefined) {
          return null;
        }
        const now = yield* Clock.currentTimeMillis;
        if (cached.expiresAt <= now) {
          yield* db.delete(cacheEvents).where(eq(cacheEvents.singleton, 1));
          return null;
        }
        return {
          data: cached.data,
          event: cached.event,
          sequence: cached.sequence,
        } satisfies CachedEvent;
      });

      const getInfo = Effect.fn("ChannelShard.getInfo")(function* () {
        const subscriptions = yield* subscriptionCount();
        return {
          cache: yield* getCachedEvent(),
          occupied: subscriptions > 0,
          subscriptionCount: subscriptions,
          userCount: yield* userCount(),
        } satisfies ChannelInfo;
      });

      const enqueue = Effect.fn("ChannelShard.enqueue")(function* (
        event: string,
        data: string,
        excludedSocketId: string | null = null,
        userId: string | null = null,
      ) {
        const createdAt = yield* Clock.currentTimeMillis;
        return yield* db.transaction(
          Effect.fn("ChannelShard.enqueueTransaction")(function* (tx) {
            const updated = yield* tx
              .update(channelState)
              .set({ sequence: sql`${channelState.sequence} + 1` })
              .where(eq(channelState.singleton, 1))
              .returning({ sequence: channelState.sequence });
            const sequence = updated[0]?.sequence;
            if (sequence === undefined) {
              return yield* failActor("enqueue", "Channel sequence state is missing");
            }
            yield* tx.insert(outboxEvents).values({
              createdAt,
              data,
              event,
              excludedSocketId,
              sequence,
              userId,
            });
            return sequence;
          }),
        );
      });

      const syncDirectory = Effect.fn("ChannelShard.syncDirectory")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        const directory = yield* directories.getByName(
          directoryShardName(placement.appId, channel),
          placement,
        );
        yield* directory.set({
          channel,
          subscriptionCount: yield* subscriptionCount(),
          userCount: yield* userCount(),
        });
      });

      const deleteOutboxEvent = Effect.fn("ChannelShard.deleteOutboxEvent")(function* (
        sequence: number,
      ) {
        yield* db.transaction(
          Effect.fn("ChannelShard.deleteOutboxTransaction")(function* (tx) {
            yield* tx.delete(branchDeliveries).where(eq(branchDeliveries.sequence, sequence));
            yield* tx.delete(outboxEvents).where(eq(outboxEvents.sequence, sequence));
          }),
        );
      });

      const deliverEvent = Effect.fn("ChannelShard.deliverEvent")(function* (outbox: OutboxEvent) {
        const metadata = yield* getMetadata();
        const branches = yield* db
          .select({ branchName: channelBranches.branchName })
          .from(channelBranches)
          .where(gt(channelBranches.subscriptionCount, 0))
          .orderBy(channelBranches.branchName);

        yield* Effect.forEach(
          branches,
          Effect.fn("ChannelShard.deliverBranch")(function* (branch) {
            const receipts = yield* db
              .select({ branchName: branchDeliveries.branchName })
              .from(branchDeliveries)
              .where(
                and(
                  eq(branchDeliveries.sequence, outbox.sequence),
                  eq(branchDeliveries.branchName, branch.branchName),
                ),
              )
              .limit(1);
            if (receipts[0] !== undefined) {
              return;
            }

            const delivery: Delivery = {
              appId: metadata.appId,
              channel: metadata.channel,
              data: outbox.data,
              event: outbox.event,
              jurisdiction: metadata.jurisdiction,
              locationHint: metadata.locationHint,
              sequence: outbox.sequence,
              ...(outbox.excludedSocketId === null
                ? {}
                : { excludedSocketId: outbox.excludedSocketId }),
              ...(outbox.userId === null ? {} : { userId: outbox.userId }),
            };
            const fanout = yield* fanouts.getByName(branch.branchName, metadata);
            yield* fanout.deliver(yield* Schema.encodeEffect(Delivery)(delivery)).pipe(
              Effect.mapError(mapActorError("FanoutShard", "deliver")),
              Effect.catch(
                Effect.fn("ChannelShard.deferDelivery")(function* (error) {
                  yield* Effect.logWarning("channel branch delivery deferred").pipe(
                    Effect.annotateLogs({
                      branchName: branch.branchName,
                      message: error.message,
                      sequence: outbox.sequence,
                    }),
                  );
                  return yield* Effect.fail(DELIVERY_DEFERRED);
                }),
              ),
            );
            yield* db
              .insert(branchDeliveries)
              .values({ branchName: branch.branchName, sequence: outbox.sequence })
              .onConflictDoNothing();
          }),
          { discard: true },
        );
      });

      const processOutboxEvent = Effect.fn("ChannelShard.processOutboxEvent")(function* (
        outbox: OutboxEvent,
      ) {
        const delivered = yield* deliverEvent(outbox).pipe(
          Effect.as(true),
          Effect.catch((error) =>
            error === DELIVERY_DEFERRED ? Effect.succeed(false) : Effect.fail(error),
          ),
        );
        const now = yield* Clock.currentTimeMillis;
        if (!delivered && now - outbox.createdAt < OUTBOX_RETENTION_MS) {
          return yield* Effect.fail(DELIVERY_DEFERRED);
        }
        yield* deleteOutboxEvent(outbox.sequence);
      });

      const pumpUnlocked = Effect.fn("ChannelShard.pumpUnlocked")(function* () {
        const events = yield* db
          .select()
          .from(outboxEvents)
          .orderBy(outboxEvents.sequence)
          .limit(100);
        yield* Effect.forEach(events, processOutboxEvent, { discard: true }).pipe(
          Effect.catch((error) => (error === DELIVERY_DEFERRED ? Effect.void : Effect.fail(error))),
        );

        const remainingRows = yield* db.select({ count: count() }).from(outboxEvents);
        if ((remainingRows[0]?.count ?? 0) === 0) {
          yield* state.storage.deleteAlarm();
        } else {
          const now = yield* Clock.currentTimeMillis;
          yield* state.storage.setAlarm(now + RETRY_DELAY_MS);
        }
      });

      const pump = Effect.fn("ChannelShard.pump")(function* () {
        const alreadyPumping = yield* Ref.getAndSet(pumpLock, true);
        if (alreadyPumping) {
          return;
        }
        yield* pumpUnlocked().pipe(Effect.ensuring(Ref.set(pumpLock, false)));
      });

      const runPumpSafely = Effect.fn("ChannelShard.runPumpSafely")(
        function* () {
          yield* pump();
        },
        Effect.catchCause(
          Effect.fn("ChannelShard.recoverPump")(function* (cause) {
            yield* Effect.logError("channel outbox pump failed").pipe(
              Effect.annotateLogs({ cause: String(cause) }),
            );
            const now = yield* Clock.currentTimeMillis;
            yield* state.storage
              .setAlarm(now + RETRY_DELAY_MS)
              .pipe(
                Effect.catchCause((alarmCause) =>
                  Effect.logError("channel outbox alarm reschedule failed").pipe(
                    Effect.annotateLogs({ cause: String(alarmCause) }),
                  ),
                ),
              );
          }),
        ),
      );

      const schedulePump = Effect.fn("ChannelShard.schedulePump")(function* () {
        yield* state.waitUntil(runPumpSafely());
      });

      const setBranch = Effect.fn("ChannelShard.setBranch")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
        branchName: string,
        branchSubscriptionCount: number,
        generation: number,
      ) {
        const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        yield* ensureMetadata(decodedPlacement, channel);
        const previousTotal = yield* subscriptionCount();
        const current = yield* db
          .select({ generation: channelBranches.generation })
          .from(channelBranches)
          .where(eq(channelBranches.branchName, branchName))
          .limit(1);

        if (current[0] === undefined || current[0].generation <= generation) {
          yield* db
            .insert(channelBranches)
            .values({
              branchName,
              generation,
              subscriptionCount: branchSubscriptionCount,
            })
            .onConflictDoUpdate({
              set: { generation, subscriptionCount: branchSubscriptionCount },
              target: channelBranches.branchName,
            });
        }

        const nextTotal = yield* subscriptionCount();
        if (nextTotal !== previousTotal && classifyChannel(channel).kind !== "presence") {
          const data = yield* encodeJson({ subscription_count: nextTotal });
          yield* enqueue("pusher_internal:subscription_count", data);
        }

        yield* syncDirectory(decodedPlacement, channel);
        yield* schedulePump();
        return {
          barrier: yield* currentSequence(),
          cache: yield* getCachedEvent(),
          subscriptionCount: nextTotal,
        } satisfies ChannelSnapshot;
      }, operationError("setBranch"));

      const joinPresence = Effect.fn("ChannelShard.joinPresence")(function* (
        join: PresenceJoinEncoded,
      ) {
        const decodedJoin = yield* Schema.decodeEffect(PresenceJoin)(join);
        yield* ensureMetadata(decodedJoin, decodedJoin.channel);
        const existing = yield* db
          .select({ userId: presenceMembers.userId })
          .from(presenceMembers)
          .where(eq(presenceMembers.socketId, decodedJoin.socketId))
          .limit(1);

        if (existing[0] === undefined) {
          const wasPresent = (yield* userConnectionCount(decodedJoin.userId)) > 0;
          yield* db.insert(presenceMembers).values({
            branchName: decodedJoin.branchName,
            socketId: decodedJoin.socketId,
            userId: decodedJoin.userId,
            userInfo: yield* encodeJson(decodedJoin.userInfo),
          });
          if (!wasPresent) {
            const data = yield* encodeJson({
              user_id: decodedJoin.userId,
              user_info: decodedJoin.userInfo,
            });
            yield* enqueue("pusher_internal:member_added", data, decodedJoin.socketId);
          }
        }

        yield* syncDirectory(decodedJoin, decodedJoin.channel);
        yield* schedulePump();
        return {
          barrier: yield* currentSequence(),
          members: yield* getPresenceMembers(),
        } satisfies PresenceSnapshot;
      }, operationError("joinPresence"));

      const leavePresence = Effect.fn("ChannelShard.leavePresence")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
        socketId: string,
      ) {
        const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        yield* ensureMetadata(decodedPlacement, channel);
        const existing = yield* db
          .select({ userId: presenceMembers.userId })
          .from(presenceMembers)
          .where(eq(presenceMembers.socketId, socketId))
          .limit(1);
        const member = existing[0];

        if (member !== undefined) {
          yield* db.delete(presenceMembers).where(eq(presenceMembers.socketId, socketId));
          if ((yield* userConnectionCount(member.userId)) === 0) {
            const data = yield* encodeJson({ user_id: member.userId });
            yield* enqueue("pusher_internal:member_removed", data, socketId);
          }
        }

        yield* syncDirectory(decodedPlacement, channel);
        yield* schedulePump();
      }, operationError("leavePresence"));

      const publish = Effect.fn("ChannelShard.publish")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
        event: string,
        data: string,
        excludedSocketId: string | null,
        userId: string | null,
        updateCache: boolean,
      ) {
        const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        yield* ensureMetadata(decodedPlacement, channel);
        const createdAt = yield* Clock.currentTimeMillis;
        yield* db.transaction(
          Effect.fn("ChannelShard.publishTransaction")(function* (tx) {
            const updated = yield* tx
              .update(channelState)
              .set({ sequence: sql`${channelState.sequence} + 1` })
              .where(eq(channelState.singleton, 1))
              .returning({ sequence: channelState.sequence });
            const sequence = updated[0]?.sequence;
            if (sequence === undefined) {
              return yield* failActor("publish", "Channel sequence state is missing");
            }
            yield* tx.insert(outboxEvents).values({
              createdAt,
              data,
              event,
              excludedSocketId,
              sequence,
              userId,
            });
            if (updateCache && classifyChannel(channel).cache) {
              yield* tx
                .insert(cacheEvents)
                .values({
                  data,
                  event,
                  expiresAt: createdAt + CACHE_RETENTION_MS,
                  sequence,
                  singleton: 1,
                })
                .onConflictDoUpdate({
                  set: {
                    data,
                    event,
                    expiresAt: createdAt + CACHE_RETENTION_MS,
                    sequence,
                  },
                  target: cacheEvents.singleton,
                });
            }
          }),
        );
        yield* schedulePump();
        return yield* getInfo();
      }, operationError("publish"));

      const info = Effect.fn("ChannelShard.info")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
      ) {
        yield* ensureMetadata(yield* Schema.decodeEffect(ApplicationPlacement)(placement), channel);
        return yield* getInfo();
      }, operationError("info"));

      const presenceUsers = Effect.fn("ChannelShard.presenceUsers")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
      ) {
        yield* ensureMetadata(yield* Schema.decodeEffect(ApplicationPlacement)(placement), channel);
        const members = yield* getPresenceMembers();
        return members.map((member) => member.userId);
      }, operationError("presenceUsers"));

      const api = {
        info,
        joinPresence,
        leavePresence,
        presenceUsers,
        publish,
        setBranch,
      } satisfies ChannelShardApi;

      return {
        ...api,
        alarm: runPumpSafely,
      };
    });
  }),
);
