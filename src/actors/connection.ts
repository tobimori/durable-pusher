import * as Cloudflare from "alchemy/Cloudflare";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { ApplicationPlacement, RuntimeApplication } from "../apps/model.ts";
import { verifyChannelAuthorization, verifyUserAuthentication } from "../pusher/crypto.ts";
import {
  ActorError,
  Attachment,
  classifyChannel,
  connectionEstablished,
  Delivery,
  decodePresenceData,
  decodeSigninData,
  decodeSubscribeData,
  decodeUnsubscribeData,
  decodeUserData,
  encodeJson,
  encodeServerEvent,
  eventDataSize,
  isServerToUserChannel,
  isValidChannelName,
  mapActorError,
  MAX_EVENT_DATA_BYTES,
  parseClientFrame,
  PresenceJoin,
  pusherError,
  subscriptionSucceeded,
  toEventData,
  type DeliveryEncoded,
  type JsonValue,
  type PresenceConnection,
  type PresenceMember,
  type ServerEvent,
} from "../pusher/protocol.ts";
import { channelShardName, makeSocketId } from "../sharding.ts";
import { ConnectionShard, type ConnectionShardApi } from "./contracts.ts";
import { ConnectionActorDependencies } from "./dependencies.ts";

export { ConnectionShard };

const CLIENT_EVENT_LIMIT = 10;
const ACTIVITY_TIMEOUT_SECONDS = 120;
const MAX_PENDING_EVENT_BYTES = 8 * 1_024 * 1_024;

const ConnectionPath = Schema.String.check(Schema.isPattern(/^\/app\/[^/]+$/));
const ProtocolVersion = Schema.FiniteFromString.pipe(
  Schema.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(5), Schema.isLessThanOrEqualTo(7)),
);

const decodeUrl = Schema.decodeUnknownEffect(Schema.URLFromString);
const decodeConnectionPath = Schema.decodeUnknownEffect(ConnectionPath);
const decodeAppKey = Schema.decodeUnknownEffect(Schema.StringFromUriComponent);
const decodeProtocolVersion = Schema.decodeUnknownEffect(ProtocolVersion);
const decodeAttachmentSchema = Schema.decodeUnknownEffect(Attachment);
const encodeAttachmentSchema = Schema.encodeEffect(Attachment);
const encodeAttachmentJson = Schema.encodeEffect(Schema.fromJsonString(Attachment));

type SocketAttachment = Attachment;
type SocketSubscription = SocketAttachment["subscriptions"][number];
type DeliveryBarrier = Pick<Delivery, "incarnation" | "sequence">;

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "ConnectionShard", message, operation });

const failActor = (operation: string, message: string) =>
  Effect.fail(actorError(operation, message));

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("ConnectionShard", operation));

const rpcError = (operation: string) =>
  Effect.catchCause((cause) => Effect.fail(actorError(operation, Cause.pretty(cause))));

const applicationPlacement = (application: ApplicationPlacement): ApplicationPlacement => ({
  appId: application.appId,
  jurisdiction: application.jurisdiction,
  locationHint: application.locationHint,
});

const findSubscription = (
  attachment: SocketAttachment,
  channel: string,
): SocketSubscription | undefined =>
  attachment.subscriptions.find((subscription) => subscription.channel === channel);

export const ConnectionShardLive = ConnectionShard.make(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const {
      applications,
      channels: channelShards,
      connectionShardSoftLimit,
    } = yield* ConnectionActorDependencies;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const registry = applications.getByName("applications");
      const socketsById = new Map<string, Cloudflare.WebSocket>();
      const socketsByChannel = new Map<string, Set<string>>();
      const activeSocketsByChannel = new Map<string, Set<string>>();
      const messageLocks = new Map<string, Semaphore.Semaphore>();
      const channelLocks = new Map<
        string,
        { readonly semaphore: Semaphore.Semaphore; users: number }
      >();
      const closingSockets = new Set<string>();
      const joiningSubscriptions = new Set<string>();
      const pendingEvents = new Map<string, Array<Delivery>>();
      const pendingEventSizes = new Map<string, number>();
      let pendingEventBytes = 0;
      const deliveryLock = yield* Semaphore.make(1);
      let cachedApplication: RuntimeApplication | undefined;
      let disabledApplication = false;
      let applicationGeneration = 0;

      yield* state.setWebSocketAutoResponse(
        new WebSocketRequestResponsePair(
          '{"event":"pusher:ping","data":{}}',
          '{"event":"pusher:pong","data":{}}',
        ),
      );

      const decodeAttachment = Effect.fn("ConnectionShard.decodeAttachment")(function* (
        socket: Cloudflare.WebSocket,
        operation: string,
      ) {
        return yield* decodeAttachmentSchema(socket.deserializeAttachment<unknown>()).pipe(
          operationError(operation),
        );
      });

      const encodeAttachment = Effect.fn("ConnectionShard.encodeAttachment")(function* (
        attachment: SocketAttachment,
        operation: string,
      ) {
        return yield* encodeAttachmentSchema(attachment).pipe(operationError(operation));
      });

      const saveAttachment = Effect.fn("ConnectionShard.saveAttachment")(function* (
        socket: Cloudflare.WebSocket,
        attachment: SocketAttachment,
        operation: string,
      ) {
        const serialized = yield* encodeAttachmentJson(attachment).pipe(operationError(operation));
        if (eventDataSize(serialized) > 16_384) {
          return yield* failActor(operation, "Connection subscription state exceeds 16 KB");
        }
        socket.serializeAttachment(yield* encodeAttachment(attachment, operation));
      });

      const addChannelSocket = (channel: string, socketId: string): void => {
        const sockets = socketsByChannel.get(channel);
        if (sockets === undefined) {
          socketsByChannel.set(channel, new Set([socketId]));
        } else {
          sockets.add(socketId);
        }
      };

      const removeChannelSocket = (channel: string, socketId: string): void => {
        const sockets = socketsByChannel.get(channel);
        if (sockets === undefined) {
          return;
        }
        sockets.delete(socketId);
        if (sockets.size === 0) {
          socketsByChannel.delete(channel);
        }
      };

      const addActiveChannelSocket = (channel: string, socketId: string): void => {
        const sockets = activeSocketsByChannel.get(channel);
        if (sockets === undefined) {
          activeSocketsByChannel.set(channel, new Set([socketId]));
        } else {
          sockets.add(socketId);
        }
      };

      const removeActiveChannelSocket = (channel: string, socketId: string): void => {
        const sockets = activeSocketsByChannel.get(channel);
        if (sockets === undefined) {
          return;
        }
        sockets.delete(socketId);
        if (sockets.size === 0) {
          activeSocketsByChannel.delete(channel);
        }
      };

      const indexSocket = (socket: Cloudflare.WebSocket, attachment: SocketAttachment): void => {
        socketsById.set(attachment.socketId, socket);
        messageLocks.set(attachment.socketId, Semaphore.makeUnsafe(1));
        for (const subscription of attachment.subscriptions) {
          addChannelSocket(subscription.channel, attachment.socketId);
          if (subscription.state === "active") {
            addActiveChannelSocket(subscription.channel, attachment.socketId);
          }
        }
      };

      for (const socket of yield* state.getWebSockets()) {
        const attachment = yield* decodeAttachment(socket, "restore").pipe(Effect.result);
        if (Result.isSuccess(attachment)) {
          const restored: SocketAttachment = {
            ...attachment.success,
            subscriptions: attachment.success.subscriptions.filter(
              (subscription) => subscription.state === "active",
            ),
          };
          yield* saveAttachment(socket, restored, "restore").pipe(Effect.orDie);
          indexSocket(socket, restored);
        }
      }

      const activeApplication = Effect.fn("ConnectionShard.activeApplication")(function* (
        placement: ApplicationPlacement,
        operation: string,
      ) {
        if (disabledApplication) {
          return yield* failActor(operation, "Application is disabled or does not exist");
        }
        if (
          cachedApplication !== undefined &&
          cachedApplication.appId === placement.appId &&
          Equal.equals(cachedApplication.jurisdiction, placement.jurisdiction) &&
          Equal.equals(cachedApplication.locationHint, placement.locationHint)
        ) {
          return cachedApplication;
        }
        const encoded = Option.fromNullishOr(yield* registry.resolveById(placement.appId));
        if (Option.isNone(encoded)) {
          return yield* failActor(operation, "Application is disabled or does not exist");
        }
        const application = yield* Schema.decodeEffect(RuntimeApplication)(encoded.value).pipe(
          operationError(operation),
        );
        if (
          !Equal.equals(application.jurisdiction, placement.jurisdiction) ||
          !Equal.equals(application.locationHint, placement.locationHint)
        ) {
          return yield* failActor(operation, "Application is disabled or does not exist");
        }
        cachedApplication = application;
        return application;
      });

      const send = Effect.fn("ConnectionShard.send")(function* (
        socket: Cloudflare.WebSocket,
        event: ServerEvent,
      ) {
        yield* socket.send(yield* encodeServerEvent(event));
      }, operationError("send"));

      const logNativeError = Effect.fn("ConnectionShard.logNativeError")(function* (
        operation: string,
        error: { readonly message: string },
      ) {
        yield* Effect.logWarning("Connection shard native handler failed").pipe(
          Effect.annotateLogs({
            actor: "ConnectionShard",
            error: error.message,
            operation,
          }),
        );
      });

      const sendNative = Effect.fn("ConnectionShard.sendNative")(function* (
        socket: Cloudflare.WebSocket,
        event: ServerEvent,
        operation: string,
      ) {
        const result = yield* send(socket, event).pipe(Effect.result);
        if (Result.isFailure(result)) {
          yield* logNativeError(operation, result.failure);
        }
      });

      const closeNative = Effect.fn("ConnectionShard.closeNative")(function* (
        socket: Cloudflare.WebSocket,
        code: number,
        reason: string,
        operation: string,
      ) {
        yield* socket.close(code, reason).pipe(
          Effect.catchCause(
            Effect.fn("ConnectionShard.closeNative.handleError")(function* (cause) {
              yield* Effect.logWarning("Connection shard socket close failed").pipe(
                Effect.annotateLogs({
                  actor: "ConnectionShard",
                  error: Cause.pretty(cause),
                  operation,
                }),
              );
            }),
          ),
        );
      });

      const rejectConnection = Effect.fn("ConnectionShard.rejectConnection")(function* (
        socket: Cloudflare.WebSocket,
        protocol: Option.Option<number>,
        code: number,
        message: string,
      ) {
        if (Option.contains(protocol, 5)) {
          yield* sendNative(socket, pusherError(message, code), "rejectConnection");
        }
        yield* closeNative(socket, code, message, "rejectConnection");
      });

      const encodedPlacement = Effect.fn("ConnectionShard.encodedPlacement")(function* (
        placement: ApplicationPlacement,
      ) {
        return yield* Schema.encodeEffect(ApplicationPlacement)(placement);
      });

      const channelActor = Effect.fn("ConnectionShard.channelActor")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        return yield* channelShards.getByName(
          channelShardName(placement.appId, channel),
          placement,
        );
      });

      const registerChannel = Effect.fn("ConnectionShard.registerChannel")(function* (
        placement: ApplicationPlacement,
        channel: string,
        gatewayName: string,
        registrationToken: string,
      ) {
        const actor = yield* channelActor(placement, channel);
        return yield* actor.registerGateway(
          yield* encodedPlacement(placement),
          channel,
          gatewayName,
          registrationToken,
        );
      }, operationError("registerChannel"));

      const channelSnapshot = Effect.fn("ConnectionShard.channelSnapshot")(function* (
        placement: ApplicationPlacement,
        channel: string,
      ) {
        const actor = yield* channelActor(placement, channel);
        return yield* actor.snapshot(yield* encodedPlacement(placement), channel);
      }, operationError("channelSnapshot"));

      const unregisterChannel = Effect.fn("ConnectionShard.unregisterChannel")(function* (
        placement: ApplicationPlacement,
        channel: string,
        gatewayName: string,
        registrationToken: string,
      ) {
        const actor = yield* channelActor(placement, channel);
        yield* actor.unregisterGateway(
          yield* encodedPlacement(placement),
          channel,
          gatewayName,
          registrationToken,
        );
      }, operationError("unregisterChannel"));

      const currentRegistrationToken = Effect.fn("ConnectionShard.currentRegistrationToken")(
        function* (channel: string) {
          for (const socketId of socketsByChannel.get(channel) ?? []) {
            const socket = socketsById.get(socketId);
            if (socket === undefined) {
              continue;
            }
            const attachment = yield* decodeAttachment(socket, "currentRegistrationToken");
            const subscription = findSubscription(attachment, channel);
            if (subscription !== undefined) {
              return subscription.registrationToken;
            }
          }
          return undefined;
        },
      );

      const presenceSubscriptionData = Effect.fn("ConnectionShard.presenceSubscriptionData")(
        function* (members: ReadonlyArray<PresenceMember>) {
          const hash: Record<string, JsonValue> = {};
          for (const member of members) {
            hash[member.userId] = member.userInfo;
          }
          return yield* encodeJson({
            presence: {
              count: members.length,
              hash,
              ids: members.map((member) => member.userId),
            },
          });
        },
        operationError("presenceSubscriptionData"),
      );

      const pendingKey = (socketId: string, channel: string): string => `${socketId}\0${channel}`;

      const takePending = (key: string): Array<Delivery> => {
        const events = pendingEvents.get(key) ?? [];
        pendingEvents.delete(key);
        pendingEventBytes -= pendingEventSizes.get(key) ?? 0;
        pendingEventSizes.delete(key);
        return events;
      };

      const clearPending = (key: string): void => {
        takePending(key);
      };

      const withChannelLock = <A, E, R>(
        channel: string,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, E, R> => {
        let entry = channelLocks.get(channel);
        if (entry === undefined) {
          entry = { semaphore: Semaphore.makeUnsafe(1), users: 0 };
          channelLocks.set(channel, entry);
        }
        entry.users += 1;
        return effect.pipe(
          Semaphore.withPermit(entry.semaphore),
          Effect.ensuring(
            Effect.sync(() => {
              entry.users -= 1;
              if (entry.users === 0 && channelLocks.get(channel) === entry) {
                channelLocks.delete(channel);
              }
            }),
          ),
        );
      };

      const messageLock = (socketId: string): Semaphore.Semaphore => {
        const existing = messageLocks.get(socketId);
        if (existing !== undefined) {
          return existing;
        }
        const created = Semaphore.makeUnsafe(1);
        messageLocks.set(socketId, created);
        return created;
      };

      const flushPending = Effect.fn("ConnectionShard.flushPending")(function* (
        socket: Cloudflare.WebSocket,
        attachment: SocketAttachment,
        channel: string,
        barriers: ReadonlyArray<DeliveryBarrier>,
      ) {
        const key = pendingKey(attachment.socketId, channel);
        const events = takePending(key);
        let latestIncarnation = 0;
        for (const barrier of barriers) {
          latestIncarnation = Math.max(latestIncarnation, barrier.incarnation);
        }
        for (const event of events) {
          if (event.incarnation < latestIncarnation) {
            continue;
          }
          if (
            barriers.some(
              (barrier) =>
                barrier.incarnation === event.incarnation && event.sequence <= barrier.sequence,
            )
          ) {
            continue;
          }
          yield* send(socket, {
            channel,
            data: event.data,
            event: event.event,
            ...(event.userId === undefined ? {} : { user_id: event.userId }),
          });
        }
      });

      const rollbackSubscription = Effect.fn("ConnectionShard.rollbackSubscription")(function* (
        socket: Cloudflare.WebSocket,
        attachment: SocketAttachment,
        channel: string,
        placement: ApplicationPlacement,
        presenceStarted: boolean,
      ) {
        const current = yield* decodeAttachment(socket, "rollbackSubscription");
        const subscription = findSubscription(current, channel);
        if (
          presenceStarted &&
          subscription?.kind === "presence" &&
          subscription.userId !== undefined
        ) {
          const actor = yield* channelActor(placement, channel);
          yield* actor.leavePresence(
            yield* encodedPlacement(placement),
            channel,
            current.socketId,
            subscription.userId,
            subscription.state === "active",
          );
        }
        const latest = yield* decodeAttachment(socket, "rollbackSubscription");
        const rolledBack: SocketAttachment = {
          ...latest,
          subscriptions: latest.subscriptions.filter(
            (subscription) => subscription.channel !== channel,
          ),
        };
        yield* saveAttachment(socket, rolledBack, "rollbackSubscription");
        removeChannelSocket(channel, attachment.socketId);
        removeActiveChannelSocket(channel, attachment.socketId);
        const key = pendingKey(attachment.socketId, channel);
        joiningSubscriptions.delete(key);
        clearPending(key);
        if (!socketsByChannel.has(channel)) {
          if (subscription !== undefined) {
            yield* unregisterChannel(
              placement,
              channel,
              current.shardName,
              subscription.registrationToken,
            ).pipe(Effect.result);
          }
        }
      });

      const subscribe = Effect.fn("ConnectionShard.subscribe")(function* (
        socket: Cloudflare.WebSocket,
        rawData: JsonValue | undefined,
      ) {
        const attachment = yield* decodeAttachment(socket, "subscribe");
        const application = yield* activeApplication(attachment, "subscribe");
        const data = yield* decodeSubscribeData(rawData);
        const channel = data.channel;
        const serverToUser = isServerToUserChannel(channel);
        if ((!serverToUser && !isValidChannelName(channel)) || channel.length > 164) {
          return yield* failActor("subscribe", "Invalid channel name");
        }
        if (serverToUser && channel !== `#server-to-user-${attachment.userId ?? ""}`) {
          return yield* failActor(
            "subscribe",
            "Server-to-user channel requires matching user sign-in",
          );
        }

        const existing = findSubscription(attachment, channel);
        if (existing?.state === "active") {
          yield* send(socket, subscriptionSucceeded(channel));
          return;
        }

        const channelType = classifyChannel(channel);
        let presence: { readonly userId: string; readonly userInfo: JsonValue } | undefined;
        if (!serverToUser && channelType.kind !== "public") {
          const authorization = data.auth ?? "";
          if (channelType.kind === "presence") {
            if (data.channel_data !== undefined) {
              if (
                !verifyChannelAuthorization(
                  authorization,
                  application.appKey,
                  application.appSecret,
                  attachment.socketId,
                  channel,
                  data.channel_data,
                )
              ) {
                return yield* failActor("subscribe", "Invalid channel authorization");
              }
              presence = yield* decodePresenceData(data.channel_data);
            } else {
              if (attachment.userId === undefined || attachment.userData === undefined) {
                return yield* failActor(
                  "subscribe",
                  "Presence subscription requires signed user data",
                );
              }
              if (
                !verifyChannelAuthorization(
                  authorization,
                  application.appKey,
                  application.appSecret,
                  attachment.socketId,
                  channel,
                )
              ) {
                return yield* failActor(
                  "subscribe",
                  "Presence subscription requires signed user data",
                );
              }
              const user = yield* decodeUserData(attachment.userData);
              presence = { userId: user.id, userInfo: user.user_info ?? null };
            }
          } else if (
            !verifyChannelAuthorization(
              authorization,
              application.appKey,
              application.appSecret,
              attachment.socketId,
              channel,
            )
          ) {
            return yield* failActor("subscribe", "Invalid channel authorization");
          }
        }

        const placement = applicationPlacement(attachment);
        const preparedResult = yield* Effect.gen(function* () {
          const firstLocal = !socketsByChannel.has(channel);
          const registrationToken = firstLocal
            ? yield* makeSocketId()
            : ((yield* currentRegistrationToken(channel)) ?? (yield* makeSocketId()));
          const joining: SocketSubscription = {
            channel,
            kind: channelType.kind,
            registrationToken,
            state: "joining",
            ...(presence === undefined
              ? {}
              : { userId: presence.userId, userInfo: presence.userInfo }),
          };
          const joiningAttachment: SocketAttachment = {
            ...attachment,
            subscriptions: [
              ...attachment.subscriptions.filter(
                (subscription) => subscription.channel !== channel,
              ),
              joining,
            ],
          };
          yield* saveAttachment(socket, joiningAttachment, "subscribe");
          addChannelSocket(channel, attachment.socketId);
          joiningSubscriptions.add(pendingKey(attachment.socketId, channel));

          const snapshotResult = yield* (
            firstLocal
              ? registerChannel(placement, channel, attachment.shardName, registrationToken)
              : channelSnapshot(placement, channel)
          ).pipe(Effect.result);
          if (Result.isFailure(snapshotResult)) {
            const rolledBack = yield* rollbackSubscription(
              socket,
              joiningAttachment,
              channel,
              placement,
              false,
            ).pipe(Effect.result);
            if (Result.isFailure(rolledBack)) {
              closingSockets.add(attachment.socketId);
              yield* closeNative(socket, 1011, "Subscription failed", "subscribe");
              return yield* rolledBack.failure;
            }
            return yield* snapshotResult.failure;
          }
          return { attachment: joiningAttachment, snapshot: snapshotResult.success };
        }).pipe((effect) => withChannelLock(channel, effect), Effect.result);
        if (Result.isFailure(preparedResult)) {
          return yield* preparedResult.failure;
        }
        const joiningAttachment = preparedResult.success.attachment;
        const snapshot = preparedResult.success.snapshot;
        const barriers: DeliveryBarrier[] = [
          { incarnation: snapshot.incarnation, sequence: snapshot.barrier },
        ];
        const key = pendingKey(attachment.socketId, channel);
        let cachedEvent = snapshot.cache;

        let presenceMembers: ReadonlyArray<PresenceMember> | undefined;
        if (presence !== undefined) {
          const actor = yield* channelActor(placement, channel);
          const joinedResult = yield* actor
            .joinPresence(
              yield* Schema.encodeEffect(PresenceJoin)({
                appId: attachment.appId,
                channel,
                jurisdiction: attachment.jurisdiction,
                locationHint: attachment.locationHint,
                socketId: attachment.socketId,
                userId: presence.userId,
                userInfo: presence.userInfo,
              }),
            )
            .pipe(Effect.result);
          if (Result.isFailure(joinedResult)) {
            const rolledBack = yield* rollbackSubscription(
              socket,
              joiningAttachment,
              channel,
              placement,
              true,
            ).pipe(Effect.result);
            if (Result.isFailure(rolledBack)) {
              closingSockets.add(attachment.socketId);
              yield* closeNative(socket, 1011, "Subscription failed", "subscribe");
              return yield* rolledBack.failure;
            }
            return yield* joinedResult.failure;
          }
          const joined = joinedResult.success;
          presenceMembers = joined.members;
          barriers.push({ incarnation: joined.incarnation, sequence: joined.barrier });
          if (channelType.cache) {
            const latest = yield* channelSnapshot(placement, channel).pipe(Effect.result);
            if (Result.isFailure(latest)) {
              const rolledBack = yield* rollbackSubscription(
                socket,
                joiningAttachment,
                channel,
                placement,
                true,
              ).pipe(Effect.result);
              closingSockets.add(attachment.socketId);
              yield* closeNative(socket, 1011, "Subscription failed", "subscribe");
              if (Result.isFailure(rolledBack)) {
                return yield* rolledBack.failure;
              }
              return yield* latest.failure;
            }
            cachedEvent = latest.success.cache;
            barriers.push({
              incarnation: latest.success.incarnation,
              sequence: latest.success.barrier,
            });
          }
        }

        const activeAttachment: SocketAttachment = {
          ...joiningAttachment,
          subscriptions: joiningAttachment.subscriptions.map((subscription) =>
            subscription.channel === channel ? { ...subscription, state: "active" } : subscription,
          ),
        };
        const activated = yield* Effect.gen(function* () {
          yield* saveAttachment(socket, activeAttachment, "subscribe");
          if (cachedEvent !== null) {
            yield* send(socket, {
              channel,
              data: cachedEvent.data,
              event: cachedEvent.event,
            });
          } else if (channelType.cache) {
            yield* send(socket, { channel, data: "{}", event: "pusher:cache_miss" });
          }
          yield* send(
            socket,
            subscriptionSucceeded(
              channel,
              presenceMembers === undefined
                ? "{}"
                : yield* presenceSubscriptionData(presenceMembers),
            ),
          );
          addActiveChannelSocket(channel, attachment.socketId);
          if (channelType.kind !== "presence") {
            const actor = yield* channelActor(placement, channel);
            yield* actor.broadcastSubscriptionCount(yield* encodedPlacement(placement), channel);
          }
          yield* Effect.gen(function* () {
            joiningSubscriptions.delete(key);
            yield* flushPending(socket, activeAttachment, channel, barriers);
          }).pipe(Semaphore.withPermit(deliveryLock));
        }).pipe(
          Effect.ensuring(Effect.sync(() => joiningSubscriptions.delete(key))),
          Effect.result,
        );
        if (Result.isFailure(activated)) {
          clearPending(key);
          yield* rollbackSubscription(
            socket,
            activeAttachment,
            channel,
            placement,
            presence !== undefined,
          ).pipe(Effect.result);
          closingSockets.add(attachment.socketId);
          yield* closeNative(socket, 1011, "Subscription failed", "subscribe");
          return yield* activated.failure;
        }
      }, operationError("subscribe"));

      const unsubscribe = Effect.fn("ConnectionShard.unsubscribe")(function* (
        socket: Cloudflare.WebSocket,
        rawData: JsonValue | undefined,
      ) {
        const attachment = yield* decodeAttachment(socket, "unsubscribe");
        const data = yield* decodeUnsubscribeData(rawData);
        const current = findSubscription(attachment, data.channel);
        if (current === undefined) {
          return;
        }
        const placement = applicationPlacement(attachment);
        const failures: ActorError[] = [];
        if (current.kind === "presence" && current.userId !== undefined) {
          const userId = current.userId;
          const result = yield* Effect.gen(function* () {
            const actor = yield* channelActor(placement, data.channel);
            yield* actor.leavePresence(
              yield* encodedPlacement(placement),
              data.channel,
              attachment.socketId,
              userId,
              current.state === "active",
            );
          }).pipe(operationError("unsubscribe"), Effect.result);
          if (Result.isFailure(result)) {
            failures.push(result.failure);
          }
        }
        const nextAttachment: SocketAttachment = {
          ...attachment,
          subscriptions: attachment.subscriptions.filter(
            (subscription) => subscription.channel !== data.channel,
          ),
        };
        yield* saveAttachment(socket, nextAttachment, "unsubscribe");
        removeChannelSocket(data.channel, attachment.socketId);
        removeActiveChannelSocket(data.channel, attachment.socketId);
        const key = pendingKey(attachment.socketId, data.channel);
        joiningSubscriptions.delete(key);
        clearPending(key);
        if (!socketsByChannel.has(data.channel)) {
          const result = yield* unregisterChannel(
            placement,
            data.channel,
            attachment.shardName,
            current.registrationToken,
          ).pipe(Effect.result);
          if (Result.isFailure(result)) {
            failures.push(result.failure);
          }
        }
        if (current.kind !== "presence") {
          const actor = yield* channelActor(placement, data.channel);
          const result = yield* actor
            .broadcastSubscriptionCount(yield* encodedPlacement(placement), data.channel)
            .pipe(operationError("unsubscribe"), Effect.result);
          if (Result.isFailure(result)) {
            failures.push(result.failure);
          }
        }
        const failure = failures[0];
        if (failure !== undefined) {
          return yield* failure;
        }
      }, operationError("unsubscribe"));

      const signin = Effect.fn("ConnectionShard.signin")(function* (
        socket: Cloudflare.WebSocket,
        rawData: JsonValue | undefined,
      ) {
        const attachment = yield* decodeAttachment(socket, "signin");
        const application = yield* activeApplication(attachment, "signin");
        const data = yield* decodeSigninData(rawData);
        if (
          !verifyUserAuthentication(
            data.auth,
            application.appKey,
            application.appSecret,
            attachment.socketId,
            data.user_data,
          )
        ) {
          return yield* failActor("signin", "Invalid user authentication");
        }
        const user = yield* decodeUserData(data.user_data);
        if (attachment.userId !== undefined && attachment.userId !== user.id) {
          return yield* failActor("signin", "Connection is already signed in as another user");
        }
        yield* saveAttachment(
          socket,
          { ...attachment, userData: data.user_data, userId: user.id },
          "signin",
        );
        yield* send(socket, {
          data: yield* encodeJson({ user_data: data.user_data }),
          event: "pusher:signin_success",
        });
        if ((user.watchlist?.length ?? 0) > 100) {
          yield* send(socket, pusherError("Watchlist limit exceeded", 4302));
        }
      }, operationError("signin"));

      const takeClientEventToken = Effect.fn("ConnectionShard.takeClientEventToken")(function* (
        socket: Cloudflare.WebSocket,
        attachment: SocketAttachment,
      ) {
        const window = Math.floor((yield* Clock.currentTimeMillis) / 1_000);
        const eventCount = attachment.eventWindow === window ? attachment.eventCount + 1 : 1;
        yield* saveAttachment(
          socket,
          { ...attachment, eventCount, eventWindow: window },
          "clientEvent",
        );
        return eventCount <= CLIENT_EVENT_LIMIT;
      });

      const clientEvent = Effect.fn("ConnectionShard.clientEvent")(function* (
        socket: Cloudflare.WebSocket,
        event: string,
        channel: string | undefined,
        data: JsonValue | undefined,
      ) {
        if (channel === undefined) {
          return yield* failActor("clientEvent", "Client event requires a channel");
        }
        const attachment = yield* decodeAttachment(socket, "clientEvent");
        yield* activeApplication(attachment, "clientEvent");
        const current = findSubscription(attachment, channel);
        if (
          current?.state !== "active" ||
          (current.kind !== "private" && current.kind !== "presence") ||
          classifyChannel(channel).kind === "encrypted"
        ) {
          return yield* failActor("clientEvent", "Client event is not allowed on this channel");
        }
        if (!(yield* takeClientEventToken(socket, attachment))) {
          yield* send(socket, pusherError("Client event rate limit exceeded", 4301));
          return;
        }
        const eventData = yield* toEventData(data ?? null);
        if (eventDataSize(eventData) > MAX_EVENT_DATA_BYTES) {
          return yield* failActor("clientEvent", "Client event data exceeds 10 KB");
        }
        const actor = yield* channelActor(applicationPlacement(attachment), channel);
        yield* actor.publish(
          yield* encodedPlacement(applicationPlacement(attachment)),
          channel,
          event,
          eventData,
          attachment.socketId,
          current.kind === "presence" ? (current.userId ?? null) : null,
          false,
        );
      }, operationError("clientEvent"));

      const cleanup = Effect.fn("ConnectionShard.cleanup")(function* (
        attachment: SocketAttachment,
      ) {
        const placement = applicationPlacement(attachment);
        const failures: ActorError[] = [];
        for (const subscription of attachment.subscriptions) {
          if (subscription.kind === "presence" && subscription.userId !== undefined) {
            const userId = subscription.userId;
            const result = yield* Effect.gen(function* () {
              const actor = yield* channelActor(placement, subscription.channel);
              yield* actor.leavePresence(
                yield* encodedPlacement(placement),
                subscription.channel,
                attachment.socketId,
                userId,
                subscription.state === "active",
              );
            }).pipe(operationError("cleanup"), Effect.result);
            if (Result.isFailure(result)) {
              failures.push(result.failure);
            }
          }
        }
        socketsById.delete(attachment.socketId);
        messageLocks.delete(attachment.socketId);
        for (const subscription of attachment.subscriptions) {
          removeChannelSocket(subscription.channel, attachment.socketId);
          removeActiveChannelSocket(subscription.channel, attachment.socketId);
          const key = pendingKey(attachment.socketId, subscription.channel);
          joiningSubscriptions.delete(key);
          clearPending(key);
          if (!socketsByChannel.has(subscription.channel)) {
            const result = yield* unregisterChannel(
              placement,
              subscription.channel,
              attachment.shardName,
              subscription.registrationToken,
            ).pipe(Effect.result);
            if (Result.isFailure(result)) {
              failures.push(result.failure);
            }
          }
          if (subscription.kind !== "presence") {
            const actor = yield* channelActor(placement, subscription.channel);
            const result = yield* actor
              .broadcastSubscriptionCount(yield* encodedPlacement(placement), subscription.channel)
              .pipe(operationError("cleanup"), Effect.result);
            if (Result.isFailure(result)) {
              failures.push(result.failure);
            }
          }
        }
        closingSockets.delete(attachment.socketId);
        const failure = failures[0];
        if (failure !== undefined) {
          return yield* failure;
        }
      }, operationError("cleanup"));

      const connect = Effect.fn("ConnectionShard.connect")(function* (
        socket: Cloudflare.WebSocket,
        protocol: number,
        shardName: string,
        application: RuntimeApplication,
        expectedGeneration: number,
      ) {
        if (expectedGeneration !== applicationGeneration) {
          return yield* failActor("connect", "Application state changed during connection");
        }
        disabledApplication = false;
        cachedApplication = application;
        const socketId = yield* makeSocketId();
        const now = yield* Clock.currentTimeMillis;
        const attachment: SocketAttachment = {
          appId: application.appId,
          eventCount: 0,
          eventWindow: Math.floor(now / 1_000),
          jurisdiction: application.jurisdiction,
          locationHint: application.locationHint,
          protocol,
          shardName,
          socketId,
          subscriptions: [],
        };
        yield* saveAttachment(socket, attachment, "connect");
        indexSocket(socket, attachment);
        yield* send(socket, yield* connectionEstablished(socketId, ACTIVITY_TIMEOUT_SECONDS));
      }, operationError("connect"));

      const fetch = Effect.fn("ConnectionShard.fetch")(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.headers.upgrade?.toLowerCase() !== "websocket") {
          return HttpServerResponse.text("Expected a WebSocket upgrade", { status: 426 });
        }
        if ((yield* state.getWebSockets()).length >= connectionShardSoftLimit) {
          return HttpServerResponse.text("Connection shard is full", {
            headers: { "x-durable-pusher-shard-full": "1" },
            status: 503,
          });
        }
        const expectedGeneration = applicationGeneration;

        const [upgradeResponse, socket] = yield* Cloudflare.upgrade();
        const urlResult = yield* decodeUrl(request.originalUrl).pipe(Effect.result);
        if (Result.isFailure(urlResult)) {
          yield* rejectConnection(socket, Option.none(), 4001, "Application does not exist");
          return upgradeResponse;
        }
        const rawProtocol = Option.fromNullishOr(urlResult.success.searchParams.get("protocol"));
        const protocolResult = Option.isNone(rawProtocol)
          ? Option.none()
          : Option.some(yield* decodeProtocolVersion(rawProtocol.value).pipe(Effect.result));
        const pathResult = yield* decodeConnectionPath(urlResult.success.pathname).pipe(
          Effect.result,
        );
        const keyResult = Result.isSuccess(pathResult)
          ? yield* decodeAppKey(pathResult.success.slice(5)).pipe(Effect.result)
          : pathResult;
        const protocol = Option.flatMap(protocolResult, (result) =>
          Result.isSuccess(result) ? Option.some(result.success) : Option.none(),
        );
        const encodedApplication = Result.isSuccess(keyResult)
          ? Option.fromNullishOr(yield* registry.resolveByKey(keyResult.success))
          : Option.none();
        if (Option.isNone(encodedApplication)) {
          yield* rejectConnection(socket, protocol, 4001, "Application does not exist");
          return upgradeResponse;
        }
        const application = yield* Schema.decodeEffect(RuntimeApplication)(
          encodedApplication.value,
        ).pipe(operationError("fetch"));
        if (Option.isNone(rawProtocol)) {
          yield* rejectConnection(socket, Option.none(), 4008, "Protocol version is required");
          return upgradeResponse;
        }
        if (Option.isNone(protocol)) {
          yield* rejectConnection(socket, Option.none(), 4007, "Unsupported protocol version");
          return upgradeResponse;
        }
        const shardName = Option.fromNullishOr(
          request.headers["x-durable-pusher-connection-shard"],
        );
        if (
          Option.isNone(shardName) ||
          !shardName.value.startsWith(`${application.appId}:connection:`)
        ) {
          yield* rejectConnection(socket, protocol, 4000, "Connection routing failed");
          return upgradeResponse;
        }

        const connected = yield* connect(
          socket,
          protocol.value,
          shardName.value,
          application,
          expectedGeneration,
        ).pipe(Effect.result);
        if (Result.isFailure(connected)) {
          yield* logNativeError("fetch", connected.failure);
          yield* sendNative(socket, pusherError(connected.failure.message), "fetch");
          yield* closeNative(socket, 1011, "Connection failed", "fetch");
          return upgradeResponse;
        }
        const expectedProtocol = `pusher-channels-protocol-${protocol.value}`;
        const requestedProtocols = Option.fromNullishOr(
          request.headers["sec-websocket-protocol"],
        ).pipe(Option.map((header) => header.split(",").map((value) => value.trim())));
        return Option.isSome(requestedProtocols) &&
          requestedProtocols.value.includes(expectedProtocol)
          ? HttpServerResponse.setHeader(
              upgradeResponse,
              "Sec-WebSocket-Protocol",
              expectedProtocol,
            )
          : upgradeResponse;
      });

      const dispatchMessage = Effect.fn("ConnectionShard.dispatchMessage")(function* (
        socket: Cloudflare.WebSocket,
        message: string,
      ) {
        const frame = yield* parseClientFrame(message);
        if (frame.event === "pusher:ping") {
          yield* send(socket, { data: "{}", event: "pusher:pong" });
          return;
        }
        if (frame.event === "pusher:pong") {
          return;
        }
        if (frame.event === "pusher:subscribe") {
          yield* subscribe(socket, frame.data);
          return;
        }
        if (frame.event === "pusher:unsubscribe") {
          yield* unsubscribe(socket, frame.data);
          return;
        }
        if (frame.event === "pusher:signin") {
          yield* signin(socket, frame.data);
          return;
        }
        if (frame.event.startsWith("client-")) {
          yield* clientEvent(socket, frame.event, frame.channel, frame.data);
          return;
        }
        yield* send(socket, pusherError(`Unsupported event: ${frame.event}`));
      });

      const webSocketMessage = Effect.fn("ConnectionShard.webSocketMessage")(function* (
        socket: Cloudflare.WebSocket,
        message: string | ArrayBuffer,
      ) {
        if (typeof message !== "string") {
          yield* closeNative(
            socket,
            1003,
            "Pusher protocol requires text frames",
            "webSocketMessage",
          );
          return;
        }
        const attachment = yield* decodeAttachment(socket, "webSocketMessage").pipe(Effect.result);
        if (Result.isFailure(attachment)) {
          yield* logNativeError("webSocketMessage", attachment.failure);
          return;
        }
        socketsById.set(attachment.success.socketId, socket);
        const handled = yield* Effect.gen(function* () {
          if (closingSockets.has(attachment.success.socketId)) {
            return;
          }
          yield* dispatchMessage(socket, message);
        }).pipe(Semaphore.withPermit(messageLock(attachment.success.socketId)), Effect.result);
        if (Result.isFailure(handled)) {
          yield* logNativeError("webSocketMessage", handled.failure);
          yield* sendNative(socket, pusherError(handled.failure.message), "webSocketMessage");
        }
      });

      const webSocketClose = Effect.fn("ConnectionShard.webSocketClose")(function* (
        socket: Cloudflare.WebSocket,
        code: number,
        reason: string,
        _wasClean: boolean,
      ) {
        const decoded = yield* decodeAttachment(socket, "webSocketClose").pipe(Effect.result);
        if (Result.isFailure(decoded)) {
          yield* logNativeError("webSocketClose", decoded.failure);
        } else {
          socketsById.set(decoded.success.socketId, socket);
          const cleaned = yield* Effect.gen(function* () {
            yield* cleanup(yield* decodeAttachment(socket, "webSocketClose"));
          }).pipe(Semaphore.withPermit(messageLock(decoded.success.socketId)), Effect.result);
          if (Result.isFailure(cleaned)) {
            yield* logNativeError("webSocketClose", cleaned.failure);
          }
        }
        yield* socket.close(code, reason);
      });

      const countSubscriptions = Effect.fn("ConnectionShard.count")(
        (channel: string) => Effect.succeed(activeSocketsByChannel.get(channel)?.size ?? 0),
        rpcError("count"),
      );

      const deliverUnlocked = Effect.fn("ConnectionShard.deliverUnlocked")(function* (
        delivery: DeliveryEncoded,
      ) {
        const decoded = yield* Schema.decodeEffect(Delivery)(delivery);
        const sockets = socketsByChannel.get(decoded.channel);
        if (sockets === undefined) {
          return 0;
        }
        for (const socketId of sockets) {
          if (closingSockets.has(socketId)) {
            continue;
          }
          const socket = socketsById.get(socketId);
          if (socket === undefined) {
            continue;
          }
          const attachment = yield* decodeAttachment(socket, "deliver");
          if (attachment.appId !== decoded.appId) {
            return yield* failActor("deliver", "Connection shard application mismatch");
          }
          const subscription = findSubscription(attachment, decoded.channel);
          if (subscription === undefined || attachment.socketId === decoded.excludedSocketId) {
            continue;
          }
          const key = pendingKey(attachment.socketId, decoded.channel);
          if (subscription.state === "joining" || joiningSubscriptions.has(key)) {
            const eventBytes =
              eventDataSize(decoded.channel) +
              eventDataSize(decoded.data) +
              eventDataSize(decoded.event);
            if (pendingEventBytes + eventBytes > MAX_PENDING_EVENT_BYTES) {
              closingSockets.add(socketId);
              clearPending(key);
              yield* closeNative(socket, 1011, "Subscription backlog exceeded", "deliver");
              continue;
            }
            const events = pendingEvents.get(key);
            if (events === undefined) {
              pendingEvents.set(key, [decoded]);
            } else {
              events.push(decoded);
            }
            pendingEventBytes += eventBytes;
            pendingEventSizes.set(key, (pendingEventSizes.get(key) ?? 0) + eventBytes);
            continue;
          }
          yield* send(socket, {
            channel: decoded.channel,
            data: decoded.data,
            event: decoded.event,
            ...(decoded.userId === undefined ? {} : { user_id: decoded.userId }),
          });
        }
        return sockets.size;
      });

      const deliver = Effect.fn("ConnectionShard.deliver")(
        (delivery: DeliveryEncoded) =>
          deliverUnlocked(delivery).pipe(Semaphore.withPermit(deliveryLock)),
        rpcError("deliver"),
      );

      const presence = Effect.fn("ConnectionShard.presence")(function* (channel: string) {
        const sockets = socketsByChannel.get(channel);
        if (sockets === undefined) {
          return [];
        }
        const members: PresenceConnection[] = [];
        for (const socketId of sockets) {
          const socket = socketsById.get(socketId);
          if (socket === undefined) {
            continue;
          }
          const attachment = yield* decodeAttachment(socket, "presence");
          const subscription = findSubscription(attachment, channel);
          if (
            subscription?.kind === "presence" &&
            subscription.state === "active" &&
            subscription.userId !== undefined &&
            subscription.userInfo !== undefined
          ) {
            members.push({
              socketId: attachment.socketId,
              userId: subscription.userId,
              userInfo: subscription.userInfo,
            });
          }
        }
        return members;
      }, rpcError("presence"));

      const terminateUser = Effect.fn("ConnectionShard.terminateUser")(function* (
        appId: string,
        userId: string,
      ) {
        let total = 0;
        const failures: ActorError[] = [];
        for (const socket of yield* state.getWebSockets()) {
          const attachment = yield* decodeAttachment(socket, "terminateUser").pipe(Effect.result);
          if (
            Result.isSuccess(attachment) &&
            attachment.success.appId === appId &&
            attachment.success.userId === userId
          ) {
            socketsById.set(attachment.success.socketId, socket);
            closingSockets.add(attachment.success.socketId);
            const closed = yield* Effect.gen(function* () {
              const current = yield* decodeAttachment(socket, "terminateUser");
              if (current.appId !== appId || current.userId !== userId) {
                return 0;
              }
              yield* socket.close(4009, "Connection terminated");
              return 1;
            }).pipe(
              Semaphore.withPermit(messageLock(attachment.success.socketId)),
              rpcError("terminateUser.socket"),
              Effect.result,
            );
            if (Result.isFailure(closed)) {
              failures.push(closed.failure);
            } else {
              total += closed.success;
            }
          }
        }
        const failure = failures[0];
        if (failure !== undefined) {
          return yield* failure;
        }
        return total;
      }, rpcError("terminateUser"));

      const terminateApplication = Effect.fn("ConnectionShard.terminateApplication")(function* (
        appId: string,
      ) {
        applicationGeneration += 1;
        disabledApplication = true;
        cachedApplication = undefined;
        let total = 0;
        const failures: ActorError[] = [];
        for (const socket of yield* state.getWebSockets()) {
          const attachment = yield* decodeAttachment(socket, "terminateApplication").pipe(
            Effect.result,
          );
          if (Result.isFailure(attachment)) {
            const closed = yield* socket
              .close(4009, "Application disabled")
              .pipe(rpcError("terminateApplication.socket"), Effect.result);
            if (Result.isFailure(closed)) {
              failures.push(closed.failure);
            } else {
              total += 1;
            }
          } else if (attachment.success.appId === appId) {
            socketsById.set(attachment.success.socketId, socket);
            closingSockets.add(attachment.success.socketId);
            const closed = yield* Effect.gen(function* () {
              const current = yield* decodeAttachment(socket, "terminateApplication");
              if (current.appId !== appId) {
                return 0;
              }
              yield* socket.close(4009, "Application disabled");
              return 1;
            }).pipe(
              Semaphore.withPermit(messageLock(attachment.success.socketId)),
              rpcError("terminateApplication.socket"),
              Effect.result,
            );
            if (Result.isFailure(closed)) {
              failures.push(closed.failure);
            } else {
              total += closed.success;
            }
          }
        }
        const failure = failures[0];
        if (failure !== undefined) {
          return yield* failure;
        }
        return total;
      }, rpcError("terminateApplication"));

      const api = {
        count: countSubscriptions,
        deliver,
        presence,
        terminateApplication,
        terminateUser,
      } satisfies ConnectionShardApi;
      return {
        ...api,
        fetch: fetch(),
        webSocketClose,
        webSocketMessage,
      };
    });
  }),
);
