import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { and, count, eq } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import migrations from "../db/migrations.ts";
import { cacheEvents, channelGateways } from "../db/schema.ts";
import {
  classifyChannel,
  Delivery,
  encodeJson,
  mapActorError,
  PresenceJoin,
  type CachedEvent,
  type ChannelInfo,
  type ChannelSnapshot,
  type PresenceJoinEncoded,
  type PresenceConnection,
  type PresenceMember,
  type PresenceSnapshot,
} from "../pusher/protocol.ts";
import { directoryShardName, fanoutRelayName, RELAY_FANOUT_WIDTH } from "../sharding.ts";
import { ChannelShard, type ChannelShardApi } from "./contracts.ts";
import { ChannelActorDependencies } from "./dependencies.ts";

export { ChannelShard };

const CACHE_RETENTION_MS = 30 * 60_000;

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("ChannelShard", operation));

export const ChannelShardLive = ChannelShard.make(
  Effect.gen(function* () {
    const { connections, directories, relays } = yield* ChannelActorDependencies;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });
      const publicationLock = yield* Semaphore.make(1);
      let sequence = 0;
      const joiningPresence = new Map<string, PresenceConnection>();
      const leavingPresence = new Set<string>();

      const summarizePresence = (connections: ReadonlyArray<PresenceConnection>) => {
        const members = new Map<string, PresenceMember>();
        const counts = new Map<string, number>();
        const seen = new Set<string>();
        for (const connection of connections) {
          if (seen.has(connection.socketId)) {
            continue;
          }
          seen.add(connection.socketId);
          members.set(connection.userId, {
            userId: connection.userId,
            userInfo: connection.userInfo,
          });
          counts.set(connection.userId, (counts.get(connection.userId) ?? 0) + 1);
        }
        return {
          connections,
          counts,
          members: Array.from(members.values()).sort((left, right) =>
            left.userId < right.userId ? -1 : left.userId > right.userId ? 1 : 0,
          ),
        };
      };

      const gatewayNames = Effect.fn("ChannelShard.gatewayNames")(function* () {
        return yield* db
          .select({
            gatewayName: channelGateways.gatewayName,
            registrationToken: channelGateways.registrationToken,
          })
          .from(channelGateways)
          .orderBy(channelGateways.gatewayName);
      }, operationError("gatewayNames"));

      const gatewayCount = Effect.fn("ChannelShard.gatewayCount")(function* () {
        const [row] = yield* db.select({ value: count() }).from(channelGateways);
        return row?.value ?? 0;
      }, operationError("gatewayCount"));

      const getCachedEvent = Effect.fn("ChannelShard.getCachedEvent")(function* () {
        const [cached] = yield* db
          .select({
            data: cacheEvents.data,
            event: cacheEvents.event,
            expiresAt: cacheEvents.expiresAt,
            sequence: cacheEvents.sequence,
          })
          .from(cacheEvents)
          .where(eq(cacheEvents.singleton, 1))
          .limit(1);
        if (cached === undefined || cached.expiresAt <= (yield* Clock.currentTimeMillis)) {
          return null;
        }
        return {
          data: cached.data,
          event: cached.event,
          sequence: cached.sequence,
        } satisfies CachedEvent;
      }, operationError("getCachedEvent"));

      const queryCounts = Effect.fn("ChannelShard.queryCounts")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        const gateways = yield* gatewayNames();
        if (gateways.length > RELAY_FANOUT_WIDTH) {
          const relay = yield* relays.getByName(
            fanoutRelayName(placement.appId, channel, "count.root"),
            placement,
          );
          return yield* relay.count(
            yield* Schema.encodeEffect(ApplicationPlacement)(placement),
            channel,
            gateways.map((gateway) => gateway.gatewayName),
            "count.root",
          );
        }
        const counts = yield* Effect.forEach(
          gateways,
          Effect.fn("ChannelShard.queryCounts.gateway")(function* (gateway) {
            const connection = yield* connections.getByName(gateway.gatewayName, placement);
            return yield* connection.count(channel);
          }),
          { concurrency: 8 },
        );
        let total = 0;
        for (const value of counts) {
          total += value;
        }
        return total;
      }, operationError("queryCounts"));

      const queryPresence = Effect.fn("ChannelShard.queryPresence")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        const gateways = yield* gatewayNames();
        const current: PresenceConnection[] = [];
        if (gateways.length > RELAY_FANOUT_WIDTH) {
          const relay = yield* relays.getByName(
            fanoutRelayName(placement.appId, channel, "presence.root"),
            placement,
          );
          current.push(
            ...(yield* relay.presence(
              yield* Schema.encodeEffect(ApplicationPlacement)(placement),
              channel,
              gateways.map((gateway) => gateway.gatewayName),
              "presence.root",
            )),
          );
        } else {
          const results = yield* Effect.forEach(
            gateways,
            Effect.fn("ChannelShard.queryPresence.gateway")(function* (gateway) {
              const connection = yield* connections.getByName(gateway.gatewayName, placement);
              return yield* connection.presence(channel);
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          for (const result of results) {
            current.push(...result);
          }
        }
        const active: PresenceConnection[] = [];
        for (const connection of current) {
          if (!leavingPresence.has(connection.socketId)) {
            active.push(connection);
          }
        }
        for (const connection of joiningPresence.values()) {
          if (!leavingPresence.has(connection.socketId)) {
            active.push(connection);
          }
        }
        return summarizePresence(active);
      }, operationError("queryPresence"));

      const getInfo = Effect.fn("ChannelShard.getInfo")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        const subscriptionCount = yield* queryCounts(placement, channel);
        const presence =
          classifyChannel(channel).kind === "presence"
            ? yield* queryPresence(placement, channel)
            : { members: [] };
        return {
          cache: yield* getCachedEvent(),
          occupied: subscriptionCount > 0,
          subscriptionCount,
          userCount: presence.members.length,
        } satisfies ChannelInfo;
      });

      const syncDirectory = Effect.fn("ChannelShard.syncDirectory")(function* (
        placement: ApplicationPlacement,
        channel: string,
        occupied: boolean,
      ) {
        const directory = yield* directories.getByName(
          directoryShardName(placement.appId, channel),
          placement,
        );
        yield* directory.set({
          channel,
          subscriptionCount: occupied ? 1 : 0,
          userCount: 0,
        });
      }, operationError("syncDirectory"));

      const pruneGateways = Effect.fn("ChannelShard.pruneGateways")(function* (
        placement: ApplicationPlacement,
        channel: string,
        gateways: ReadonlyArray<{
          readonly gatewayName: string;
          readonly registrationToken: string;
        }>,
      ) {
        if (gateways.length === 0) {
          return;
        }
        for (const gateway of gateways) {
          yield* db
            .delete(channelGateways)
            .where(
              and(
                eq(channelGateways.gatewayName, gateway.gatewayName),
                eq(channelGateways.registrationToken, gateway.registrationToken),
              ),
            );
        }
        if ((yield* gatewayCount()) === 0) {
          yield* syncDirectory(placement, channel, false);
        }
      }, operationError("pruneGateways"));

      const deliver = Effect.fn("ChannelShard.deliver")(function* (
        placement: ApplicationPlacement,
        channel: string,
        event: string,
        data: string,
        excludedSocketId: string | null,
        userId: string | null,
      ) {
        sequence += 1;
        const delivery: Delivery = {
          appId: placement.appId,
          channel,
          data,
          event,
          jurisdiction: placement.jurisdiction,
          locationHint: placement.locationHint,
          sequence,
          ...(excludedSocketId === null ? {} : { excludedSocketId }),
          ...(userId === null ? {} : { userId }),
        };
        const encoded = yield* Schema.encodeEffect(Delivery)(delivery);
        const gateways = yield* gatewayNames();
        const staleNames: string[] = [];
        if (gateways.length <= RELAY_FANOUT_WIDTH) {
          const results = yield* Effect.forEach(
            gateways,
            Effect.fn("ChannelShard.deliver.gateway")(function* (gateway) {
              const result = yield* Effect.gen(function* () {
                const connection = yield* connections.getByName(gateway.gatewayName, placement);
                return yield* connection.deliver(encoded);
              }).pipe(Effect.result);
              if (Result.isFailure(result)) {
                yield* Effect.logWarning("Channel gateway delivery failed").pipe(
                  Effect.annotateLogs({
                    channel,
                    gatewayName: gateway.gatewayName,
                    message: result.failure.message,
                  }),
                );
              }
              return { gatewayName: gateway.gatewayName, result };
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          for (const result of results) {
            if (Result.isSuccess(result.result) && result.result.success === 0) {
              staleNames.push(result.gatewayName);
            }
          }
        } else {
          const relay = yield* relays.getByName(
            fanoutRelayName(placement.appId, channel, "root"),
            placement,
          );
          const result = yield* relay
            .deliver(
              encoded,
              gateways.map((gateway) => gateway.gatewayName),
              "root",
            )
            .pipe(Effect.result);
          if (Result.isFailure(result)) {
            yield* Effect.logWarning("Channel relay delivery failed").pipe(
              Effect.annotateLogs({ channel, message: result.failure.message }),
            );
          } else {
            staleNames.push(...result.success);
          }
        }
        const stale = gateways.filter((gateway) => staleNames.includes(gateway.gatewayName));
        yield* pruneGateways(placement, channel, stale);
      }, operationError("deliver"));

      const publishUnlocked = Effect.fn("ChannelShard.publishUnlocked")(function* (
        placement: ApplicationPlacement,
        channel: string,
        event: string,
        data: string,
        excludedSocketId: string | null,
        userId: string | null,
        updateCache: boolean,
      ) {
        if (updateCache && classifyChannel(channel).cache) {
          const now = yield* Clock.currentTimeMillis;
          yield* db
            .insert(cacheEvents)
            .values({
              data,
              event,
              expiresAt: now + CACHE_RETENTION_MS,
              sequence: sequence + 1,
              singleton: 1,
            })
            .onConflictDoUpdate({
              set: {
                data,
                event,
                expiresAt: now + CACHE_RETENTION_MS,
                sequence: sequence + 1,
              },
              target: cacheEvents.singleton,
            });
        }
        yield* deliver(placement, channel, event, data, excludedSocketId, userId);
      });

      const channelSnapshot = Effect.fn("ChannelShard.channelSnapshot")(function* () {
        return {
          barrier: sequence,
          cache: yield* getCachedEvent(),
        } satisfies ChannelSnapshot;
      });

      const registerGatewayUnlocked = Effect.fn("ChannelShard.registerGatewayUnlocked")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
        gatewayName: string,
        registrationToken: string,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        const previousCount = yield* gatewayCount();
        yield* db
          .insert(channelGateways)
          .values({ gatewayName, registrationToken })
          .onConflictDoUpdate({
            set: { registrationToken },
            target: channelGateways.gatewayName,
          });
        if (previousCount === 0) {
          yield* syncDirectory(decoded, channel, true);
        }
        return yield* channelSnapshot();
      });

      const registerGateway = Effect.fn("ChannelShard.registerGateway")(
        (
          placement: ApplicationPlacementEncoded,
          channel: string,
          gatewayName: string,
          registrationToken: string,
        ) =>
          registerGatewayUnlocked(placement, channel, gatewayName, registrationToken).pipe(
            Semaphore.withPermit(publicationLock),
          ),
        operationError("registerGateway"),
      );

      const snapshot = Effect.fn("ChannelShard.snapshot")(function* (
        placement: ApplicationPlacementEncoded,
        _channel: string,
      ) {
        yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        return yield* channelSnapshot().pipe(Semaphore.withPermit(publicationLock));
      }, operationError("snapshot"));

      const unregisterGatewayUnlocked = Effect.fn("ChannelShard.unregisterGatewayUnlocked")(
        function* (
          placement: ApplicationPlacementEncoded,
          channel: string,
          gatewayName: string,
          registrationToken: string,
        ) {
          const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
          yield* db
            .delete(channelGateways)
            .where(
              and(
                eq(channelGateways.gatewayName, gatewayName),
                eq(channelGateways.registrationToken, registrationToken),
              ),
            );
          if ((yield* gatewayCount()) === 0) {
            yield* syncDirectory(decoded, channel, false);
          }
        },
      );

      const unregisterGateway = Effect.fn("ChannelShard.unregisterGateway")(
        (
          placement: ApplicationPlacementEncoded,
          channel: string,
          gatewayName: string,
          registrationToken: string,
        ) =>
          unregisterGatewayUnlocked(placement, channel, gatewayName, registrationToken).pipe(
            Semaphore.withPermit(publicationLock),
          ),
        operationError("unregisterGateway"),
      );

      const joinPresenceUnlocked = Effect.fn("ChannelShard.joinPresenceUnlocked")(function* (
        join: PresenceJoinEncoded,
      ) {
        const decoded = yield* Schema.decodeEffect(PresenceJoin)(join);
        const placement: ApplicationPlacement = {
          appId: decoded.appId,
          jurisdiction: decoded.jurisdiction,
          locationHint: decoded.locationHint,
        };
        const before = yield* queryPresence(placement, decoded.channel);
        if (!before.counts.has(decoded.userId)) {
          yield* publishUnlocked(
            placement,
            decoded.channel,
            "pusher_internal:member_added",
            yield* encodeJson({ user_id: decoded.userId, user_info: decoded.userInfo }),
            decoded.socketId,
            null,
            false,
          );
        }
        const connection: PresenceConnection = {
          socketId: decoded.socketId,
          userId: decoded.userId,
          userInfo: decoded.userInfo,
        };
        joiningPresence.set(decoded.socketId, connection);
        const presence = summarizePresence([...before.connections, connection]);
        return { barrier: sequence, members: presence.members } satisfies PresenceSnapshot;
      });

      const joinPresence = Effect.fn("ChannelShard.joinPresence")(
        (join: PresenceJoinEncoded) =>
          joinPresenceUnlocked(join).pipe(Semaphore.withPermit(publicationLock)),
        operationError("joinPresence"),
      );

      const leavePresenceUnlocked = Effect.fn("ChannelShard.leavePresenceUnlocked")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
        socketId: string,
        userId: string,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        const before = yield* queryPresence(decoded, channel);
        const wasPresent = before.connections.some(
          (connection) => connection.socketId === socketId && connection.userId === userId,
        );
        joiningPresence.delete(socketId);
        leavingPresence.add(socketId);
        const presence = yield* queryPresence(decoded, channel);
        if (wasPresent && !presence.counts.has(userId)) {
          yield* publishUnlocked(
            decoded,
            channel,
            "pusher_internal:member_removed",
            yield* encodeJson({ user_id: userId }),
            socketId,
            null,
            false,
          );
        }
      });

      const leavePresence = Effect.fn("ChannelShard.leavePresence")(
        (
          placement: ApplicationPlacementEncoded,
          channel: string,
          socketId: string,
          userId: string,
        ) =>
          leavePresenceUnlocked(placement, channel, socketId, userId).pipe(
            Semaphore.withPermit(publicationLock),
          ),
        operationError("leavePresence"),
      );

      const settlePresence = Effect.fn("ChannelShard.settlePresence")(
        (_placement: ApplicationPlacementEncoded, _channel: string, socketId: string) =>
          Effect.sync(() => {
            joiningPresence.delete(socketId);
            leavingPresence.delete(socketId);
          }),
        operationError("settlePresence"),
      );

      const publish = Effect.fn("ChannelShard.publish")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
        event: string,
        data: string,
        excludedSocketId: string | null,
        userId: string | null,
        updateCache: boolean,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        yield* publishUnlocked(
          decoded,
          channel,
          event,
          data,
          excludedSocketId,
          userId,
          updateCache,
        ).pipe(Semaphore.withPermit(publicationLock));
      }, operationError("publish"));

      const broadcastSubscriptionCount = Effect.fn("ChannelShard.broadcastSubscriptionCount")(
        (placement: ApplicationPlacementEncoded, channel: string) =>
          Effect.gen(function* () {
            const decoded = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
            yield* deliver(
              decoded,
              channel,
              "pusher_internal:subscription_count",
              yield* encodeJson({ subscription_count: yield* queryCounts(decoded, channel) }),
              null,
              null,
            );
          }).pipe(Semaphore.withPermit(publicationLock)),
        operationError("broadcastSubscriptionCount"),
      );

      const info = Effect.fn("ChannelShard.info")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
      ) {
        return yield* getInfo(yield* Schema.decodeEffect(ApplicationPlacement)(placement), channel);
      }, operationError("info"));

      const presenceUsers = Effect.fn("ChannelShard.presenceUsers")(function* (
        placement: ApplicationPlacementEncoded,
        channel: string,
      ) {
        const presence = yield* queryPresence(
          yield* Schema.decodeEffect(ApplicationPlacement)(placement),
          channel,
        );
        return presence.members.map((member) => member.userId);
      }, operationError("presenceUsers"));

      const api = {
        broadcastSubscriptionCount,
        info,
        joinPresence,
        leavePresence,
        presenceUsers,
        publish,
        registerGateway,
        settlePresence,
        snapshot,
        unregisterGateway,
      } satisfies ChannelShardApi;
      return api;
    });
  }),
);
