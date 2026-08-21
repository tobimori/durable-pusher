import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { and, count, eq, lt, sql } from "drizzle-orm";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { AppConfig } from "../config.ts";
import migrations from "../db/migrations.ts";
import {
  channelVersions,
  connections,
  pendingEvents,
  subscriptions,
  userVersions,
} from "../db/schema.ts";
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
  pusherError,
  subscriptionCount,
  subscriptionSucceeded,
  toEventData,
  type JsonValue,
  type PresenceMember,
  type ServerEvent,
} from "../pusher/protocol.ts";
import { channelShardName, fanoutShardName, makeSocketId, userShardName } from "../sharding.ts";
import {
  ConnectionShard,
  type ConnectionShardApi,
} from "./contracts.ts";
import { ConnectionActorDependencies } from "./dependencies.ts";

export { ConnectionShard };

const CLIENT_EVENT_LIMIT = 10;
const ACTIVITY_TIMEOUT_SECONDS = 120;

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

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "ConnectionShard", message, operation });

const failActor = (operation: string, message: string) =>
  Effect.fail(actorError(operation, message));

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("ConnectionShard", operation));

const rpcError = (operation: string) =>
  Effect.catchCause((cause) => Effect.fail(actorError(operation, Cause.pretty(cause))));

export const ConnectionShardLive = ConnectionShard.make(
  Effect.gen(function* () {
    const config = yield* AppConfig;
    const state = yield* Cloudflare.DurableObjectState;
    const {
      channels: channelShards,
      fanouts: fanoutShards,
      users: userShards,
    } = yield* ConnectionActorDependencies;

    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      const decodeAttachment = Effect.fn("ConnectionShard.decodeAttachment")(function* (
        socket: Cloudflare.WebSocket,
        operation: string,
      ) {
        return yield* decodeAttachmentSchema(socket.deserializeAttachment<unknown>()).pipe(
          operationError(operation),
        );
      });

      const encodeAttachment = Effect.fn("ConnectionShard.encodeAttachment")(function* (
        attachment: Attachment,
        operation: string,
      ) {
        return yield* encodeAttachmentSchema(attachment).pipe(operationError(operation));
      });

      const send = Effect.fn("ConnectionShard.send")(function* (
        socket: Cloudflare.WebSocket,
        event: ServerEvent,
      ) {
        const frame = yield* encodeServerEvent(event);
        yield* socket.send(frame);
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
        protocol: number | undefined,
        code: number,
        message: string,
      ) {
        if (protocol === 5) {
          yield* sendNative(socket, pusherError(message, code), "rejectConnection");
        }
        yield* closeNative(socket, code, message, "rejectConnection");
      });

      const socketFor = Effect.fn("ConnectionShard.socketFor")(function* (socketId: string) {
        for (const socket of yield* state.getWebSockets()) {
          const decoded = yield* decodeAttachment(socket, "socketFor").pipe(Effect.result);
          if (Result.isSuccess(decoded) && decoded.success.socketId === socketId) {
            return socket;
          }
        }
        return undefined;
      });

      const connection = Effect.fn("ConnectionShard.connection")(function* (socketId: string) {
        const rows = yield* db
          .select({ userData: connections.userData, userId: connections.userId })
          .from(connections)
          .where(eq(connections.socketId, socketId))
          .limit(1);
        return rows[0];
      }, operationError("connection"));

      const subscription = Effect.fn("ConnectionShard.subscription")(function* (
        socketId: string,
        channel: string,
      ) {
        const rows = yield* db
          .select()
          .from(subscriptions)
          .where(and(eq(subscriptions.socketId, socketId), eq(subscriptions.channel, channel)))
          .limit(1);
        return rows[0];
      }, operationError("subscription"));

      const channelSubscriptionCount = Effect.fn("ConnectionShard.channelSubscriptionCount")(
        function* (channel: string) {
          const rows = yield* db
            .select({ value: count() })
            .from(subscriptions)
            .where(eq(subscriptions.channel, channel));
          return rows[0]?.value ?? 0;
        },
        operationError("channelSubscriptionCount"),
      );

      const bumpChannelGeneration = Effect.fn("ConnectionShard.bumpChannelGeneration")(function* (
        channel: string,
      ) {
        yield* db
          .insert(channelVersions)
          .values({ channel, generation: 1 })
          .onConflictDoUpdate({
            target: channelVersions.channel,
            set: { generation: sql`${channelVersions.generation} + 1` },
          });
        const rows = yield* db
          .select({ generation: channelVersions.generation })
          .from(channelVersions)
          .where(eq(channelVersions.channel, channel))
          .limit(1);
        const generation = rows[0]?.generation;
        if (generation === undefined) {
          return yield* failActor("bumpChannelGeneration", "Channel generation is missing");
        }
        return generation;
      }, operationError("bumpChannelGeneration"));

      const bumpUserGeneration = Effect.fn("ConnectionShard.bumpUserGeneration")(function* (
        userId: string,
      ) {
        yield* db
          .insert(userVersions)
          .values({ generation: 1, userId })
          .onConflictDoUpdate({
            target: userVersions.userId,
            set: { generation: sql`${userVersions.generation} + 1` },
          });
        const rows = yield* db
          .select({ generation: userVersions.generation })
          .from(userVersions)
          .where(eq(userVersions.userId, userId))
          .limit(1);
        const generation = rows[0]?.generation;
        if (generation === undefined) {
          return yield* failActor("bumpUserGeneration", "User generation is missing");
        }
        return generation;
      }, operationError("bumpUserGeneration"));

      const syncChannel = Effect.fn("ConnectionShard.syncChannel")(function* (
        appId: string,
        channel: string,
        gateway: string,
      ) {
        const branch = fanoutShardName(appId, channel, gateway);
        return yield* fanoutShards
          .getByName(branch)
          .setGateway(
            appId,
            channel,
            branch,
            gateway,
            yield* channelSubscriptionCount(channel),
            yield* bumpChannelGeneration(channel),
          );
      }, operationError("syncChannel"));

      const syncUser = Effect.fn("ConnectionShard.syncUser")(function* (
        appId: string,
        userId: string,
        gateway: string,
      ) {
        const rows = yield* db
          .select({ value: count() })
          .from(connections)
          .where(eq(connections.userId, userId));
        yield* userShards
          .getByName(userShardName(appId, userId))
          .setGateway(gateway, rows[0]?.value ?? 0, yield* bumpUserGeneration(userId));
      }, operationError("syncUser"));

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

      const flushPending = Effect.fn("ConnectionShard.flushPending")(function* (
        socketId: string,
        channel: string,
      ) {
        const socket = yield* socketFor(socketId);
        if (socket === undefined) {
          return;
        }
        const current = yield* subscription(socketId, channel);
        if (current === undefined) {
          return;
        }
        const events = yield* db
          .select()
          .from(pendingEvents)
          .where(and(eq(pendingEvents.socketId, socketId), eq(pendingEvents.channel, channel)))
          .orderBy(pendingEvents.sequence);
        let lastSequence = current.lastSequence;
        for (const event of events) {
          if (event.sequence >= current.startSequence && event.sequence > lastSequence) {
            yield* send(socket, {
              channel,
              data: event.data,
              event: event.event,
              ...(event.userId === null ? {} : { user_id: event.userId }),
            });
            lastSequence = event.sequence;
            yield* db
              .update(subscriptions)
              .set({ lastSequence })
              .where(
                and(
                  eq(subscriptions.socketId, socketId),
                  eq(subscriptions.channel, channel),
                  lt(subscriptions.lastSequence, lastSequence),
                ),
              );
          }
        }
        yield* db
          .delete(pendingEvents)
          .where(and(eq(pendingEvents.socketId, socketId), eq(pendingEvents.channel, channel)));
      }, operationError("flushPending"));

      const subscribe = Effect.fn("ConnectionShard.subscribe")(function* (
        socket: Cloudflare.WebSocket,
        rawData: JsonValue | undefined,
      ) {
        const attachment = yield* decodeAttachment(socket, "subscribe");
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

        const existing = yield* subscription(attachment.socketId, channel);
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
                  config.appKey,
                  Redacted.value(config.appSecret),
                  attachment.socketId,
                  channel,
                  data.channel_data,
                )
              ) {
                return yield* failActor("subscribe", "Invalid channel authorization");
              }
              presence = yield* decodePresenceData(data.channel_data);
            } else {
              const signedIn = yield* connection(attachment.socketId);
              if (
                signedIn === undefined ||
                signedIn.userId === null ||
                signedIn.userData === null ||
                !verifyChannelAuthorization(
                  authorization,
                  config.appKey,
                  Redacted.value(config.appSecret),
                  attachment.socketId,
                  channel,
                )
              ) {
                return yield* failActor(
                  "subscribe",
                  "Presence subscription requires signed user data",
                );
              }
              const user = yield* decodeUserData(signedIn.userData);
              presence = {
                userId: user.id,
                userInfo: user.user_info ?? null,
              };
            }
          } else if (
            !verifyChannelAuthorization(
              authorization,
              config.appKey,
              Redacted.value(config.appSecret),
              attachment.socketId,
              channel,
            )
          ) {
            return yield* failActor("subscribe", "Invalid channel authorization");
          }
        }

        const gateway = attachment.shardName;
        const branchName = fanoutShardName(attachment.appId, channel, gateway);
        const userInfo = presence === undefined ? null : yield* encodeJson(presence.userInfo);

        yield* db
          .insert(subscriptions)
          .values({
            branchName,
            channel,
            kind: channelType.kind,
            lastSequence: 0,
            socketId: attachment.socketId,
            startSequence: 0,
            state: "joining",
            userId: presence?.userId ?? null,
            userInfo,
          })
          .onConflictDoUpdate({
            target: [subscriptions.socketId, subscriptions.channel],
            set: {
              branchName,
              kind: channelType.kind,
              state: "joining",
              userId: presence?.userId ?? null,
              userInfo,
            },
          });

        const snapshot = yield* syncChannel(attachment.appId, channel, attachment.shardName).pipe(
          Effect.catch(
            Effect.fn("ConnectionShard.subscribe.rollback")(function* (error) {
              yield* db
                .delete(subscriptions)
                .where(
                  and(
                    eq(subscriptions.socketId, attachment.socketId),
                    eq(subscriptions.channel, channel),
                  ),
                );
              yield* db
                .delete(pendingEvents)
                .where(
                  and(
                    eq(pendingEvents.socketId, attachment.socketId),
                    eq(pendingEvents.channel, channel),
                  ),
                );
              return yield* error;
            }),
          ),
        );

        let barrier = snapshot.barrier;
        let presenceMembers: ReadonlyArray<PresenceMember> | undefined;
        if (presence !== undefined) {
          const joined = yield* channelShards
            .getByName(channelShardName(attachment.appId, channel))
            .joinPresence({
              appId: attachment.appId,
              branchName,
              channel,
              socketId: attachment.socketId,
              userId: presence.userId,
              userInfo: presence.userInfo,
            });
          if (joined.barrier > barrier) {
            barrier = joined.barrier;
          }
          presenceMembers = joined.members;
        }

        if (snapshot.cache !== null) {
          yield* send(socket, {
            channel,
            data: snapshot.cache.data,
            event: snapshot.cache.event,
          });
        } else if (channelType.cache) {
          yield* send(socket, {
            channel,
            data: "{}",
            event: "pusher:cache_miss",
          });
        }

        const successData =
          presenceMembers === undefined ? "{}" : yield* presenceSubscriptionData(presenceMembers);
        yield* send(socket, subscriptionSucceeded(channel, successData));
        yield* db
          .update(subscriptions)
          .set({
            lastSequence: barrier,
            startSequence: barrier + 1,
            state: "active",
          })
          .where(
            and(
              eq(subscriptions.socketId, attachment.socketId),
              eq(subscriptions.channel, channel),
            ),
          );
        yield* flushPending(attachment.socketId, channel);

        if (channelType.kind !== "presence") {
          const countEvent = yield* subscriptionCount(channel, snapshot.subscriptionCount);
          yield* send(socket, countEvent);
        }
      }, operationError("subscribe"));

      const unsubscribe = Effect.fn("ConnectionShard.unsubscribe")(function* (
        socket: Cloudflare.WebSocket,
        rawData: JsonValue | undefined,
      ) {
        const attachment = yield* decodeAttachment(socket, "unsubscribe");
        const data = yield* decodeUnsubscribeData(rawData);
        const current = yield* subscription(attachment.socketId, data.channel);
        if (current === undefined) {
          return;
        }

        yield* db.transaction(
          Effect.fn("ConnectionShard.unsubscribe.transaction")(function* (tx) {
            yield* tx
              .delete(subscriptions)
              .where(
                and(
                  eq(subscriptions.socketId, attachment.socketId),
                  eq(subscriptions.channel, data.channel),
                ),
              );
            yield* tx
              .delete(pendingEvents)
              .where(
                and(
                  eq(pendingEvents.socketId, attachment.socketId),
                  eq(pendingEvents.channel, data.channel),
                ),
              );
          }),
        );

        const fanoutResult = yield* syncChannel(
          attachment.appId,
          data.channel,
          attachment.shardName,
        ).pipe(Effect.result);
        const presenceResult =
          current.kind === "presence"
            ? yield* channelShards
                .getByName(channelShardName(attachment.appId, data.channel))
                .leavePresence(attachment.appId, data.channel, attachment.socketId)
                .pipe(Effect.result)
            : Result.succeed(undefined);
        if (Result.isFailure(fanoutResult)) {
          return yield* fanoutResult.failure;
        }
        if (Result.isFailure(presenceResult)) {
          return yield* presenceResult.failure;
        }
      }, operationError("unsubscribe"));

      const signin = Effect.fn("ConnectionShard.signin")(function* (
        socket: Cloudflare.WebSocket,
        rawData: JsonValue | undefined,
      ) {
        const attachment = yield* decodeAttachment(socket, "signin");
        const data = yield* decodeSigninData(rawData);
        if (
          !verifyUserAuthentication(
            data.auth,
            config.appKey,
            Redacted.value(config.appSecret),
            attachment.socketId,
            data.user_data,
          )
        ) {
          return yield* failActor("signin", "Invalid user authentication");
        }
        const user = yield* decodeUserData(data.user_data);
        const current = yield* connection(attachment.socketId);
        if (current === undefined) {
          return yield* failActor("signin", "Connection does not exist");
        }
        if (current.userId !== null && current.userId !== user.id) {
          return yield* failActor("signin", "Connection is already signed in as another user");
        }

        yield* db
          .update(connections)
          .set({ userData: data.user_data, userId: user.id })
          .where(eq(connections.socketId, attachment.socketId));
        socket.serializeAttachment(
          yield* encodeAttachment({ ...attachment, userId: user.id }, "signin"),
        );
        yield* syncUser(attachment.appId, user.id, attachment.shardName);
        yield* send(socket, {
          data: yield* encodeJson({ user_data: data.user_data }),
          event: "pusher:signin_success",
        });
        if ((user.watchlist?.length ?? 0) > 100) {
          yield* send(socket, pusherError("Watchlist limit exceeded", 4302));
        }
      }, operationError("signin"));

      const takeClientEventToken = Effect.fn("ConnectionShard.takeClientEventToken")(function* (
        socketId: string,
      ) {
        const now = yield* Clock.currentTimeMillis;
        const window = Math.floor(now / 1_000);
        return yield* db.transaction(
          Effect.fn("ConnectionShard.takeClientEventToken.transaction")(function* (tx) {
            const rows = yield* tx
              .select({
                eventCount: connections.eventCount,
                eventWindow: connections.eventWindow,
              })
              .from(connections)
              .where(eq(connections.socketId, socketId))
              .limit(1);
            const current = rows[0];
            if (current === undefined) {
              return yield* failActor("takeClientEventToken", "Connection does not exist");
            }
            const eventCount = current.eventWindow === window ? current.eventCount + 1 : 1;
            yield* tx
              .update(connections)
              .set({ eventCount, eventWindow: window })
              .where(eq(connections.socketId, socketId));
            return eventCount <= CLIENT_EVENT_LIMIT;
          }),
        );
      }, operationError("takeClientEventToken"));

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
        const current = yield* subscription(attachment.socketId, channel);
        if (
          current?.state !== "active" ||
          (current.kind !== "private" && current.kind !== "presence") ||
          classifyChannel(channel).kind === "encrypted"
        ) {
          return yield* failActor("clientEvent", "Client event is not allowed on this channel");
        }
        if (!(yield* takeClientEventToken(attachment.socketId))) {
          yield* send(socket, pusherError("Client event rate limit exceeded", 4301));
          return;
        }
        const eventData = yield* toEventData(data ?? null);
        if (eventDataSize(eventData) > MAX_EVENT_DATA_BYTES) {
          return yield* failActor("clientEvent", "Client event data exceeds 10 KB");
        }

        yield* channelShards
          .getByName(channelShardName(attachment.appId, channel))
          .publish(
            attachment.appId,
            channel,
            event,
            eventData,
            attachment.socketId,
            current.kind === "presence" ? current.userId : null,
            false,
          );
      }, operationError("clientEvent"));

      const cleanup = Effect.fn("ConnectionShard.cleanup")(function* (attachment: Attachment) {
        const localSubscriptions = yield* db
          .select({ channel: subscriptions.channel, kind: subscriptions.kind })
          .from(subscriptions)
          .where(eq(subscriptions.socketId, attachment.socketId));
        const currentConnection = yield* connection(attachment.socketId);

        yield* db.transaction(
          Effect.fn("ConnectionShard.cleanup.transaction")(function* (tx) {
            yield* tx.delete(subscriptions).where(eq(subscriptions.socketId, attachment.socketId));
            yield* tx.delete(pendingEvents).where(eq(pendingEvents.socketId, attachment.socketId));
            yield* tx.delete(connections).where(eq(connections.socketId, attachment.socketId));
          }),
        );

        const failures: ActorError[] = [];
        for (const localSubscription of localSubscriptions) {
          const fanoutResult = yield* syncChannel(
            attachment.appId,
            localSubscription.channel,
            attachment.shardName,
          ).pipe(Effect.result);
          if (Result.isFailure(fanoutResult)) {
            failures.push(fanoutResult.failure);
          }
          if (localSubscription.kind === "presence") {
            const presenceResult = yield* channelShards
              .getByName(channelShardName(attachment.appId, localSubscription.channel))
              .leavePresence(attachment.appId, localSubscription.channel, attachment.socketId)
              .pipe(Effect.result);
            if (Result.isFailure(presenceResult)) {
              failures.push(presenceResult.failure);
            }
          }
        }
        if (currentConnection !== undefined && currentConnection.userId !== null) {
          const userResult = yield* syncUser(
            attachment.appId,
            currentConnection.userId,
            attachment.shardName,
          ).pipe(Effect.result);
          if (Result.isFailure(userResult)) {
            failures.push(userResult.failure);
          }
        }
        const failure = failures[0];
        if (failure !== undefined) {
          return yield* failure;
        }
      }, operationError("cleanup"));

      const connect = Effect.fn("ConnectionShard.connect")(function* (
        socket: Cloudflare.WebSocket,
        protocol: number,
        shardName: string,
      ) {
        const socketId = yield* makeSocketId();
        const now = yield* Clock.currentTimeMillis;
        const attachment = yield* encodeAttachment(
          {
            appId: config.appId,
            protocol,
            shardName,
            socketId,
          },
          "connect",
        );
        socket.serializeAttachment(attachment);
        yield* db.insert(connections).values({
          appId: config.appId,
          eventCount: 0,
          eventWindow: Math.floor(now / 1_000),
          protocol,
          shardName,
          socketId,
        });
        yield* send(socket, yield* connectionEstablished(socketId, ACTIVITY_TIMEOUT_SECONDS));
      }, operationError("connect"));

      const fetch = Effect.fn("ConnectionShard.fetch")(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.headers.upgrade?.toLowerCase() !== "websocket") {
          return HttpServerResponse.text("Expected a WebSocket upgrade", {
            status: 426,
          });
        }

        const [upgradeResponse, socket] = yield* Cloudflare.upgrade();
        const urlResult = yield* decodeUrl(request.originalUrl).pipe(Effect.result);
        if (Result.isFailure(urlResult)) {
          yield* rejectConnection(socket, undefined, 4001, "Application does not exist");
          return upgradeResponse;
        }

        const rawProtocol = urlResult.success.searchParams.get("protocol");
        const protocolResult =
          rawProtocol === null
            ? undefined
            : yield* decodeProtocolVersion(rawProtocol).pipe(Effect.result);
        const pathResult = yield* decodeConnectionPath(urlResult.success.pathname).pipe(
          Effect.result,
        );
        const keyResult = Result.isSuccess(pathResult)
          ? yield* decodeAppKey(pathResult.success.slice(5)).pipe(Effect.result)
          : pathResult;
        const protocol =
          protocolResult !== undefined && Result.isSuccess(protocolResult)
            ? protocolResult.success
            : undefined;

        if (Result.isFailure(keyResult) || keyResult.success !== config.appKey) {
          yield* rejectConnection(socket, protocol, 4001, "Application does not exist");
          return upgradeResponse;
        }
        if (rawProtocol === null) {
          yield* rejectConnection(socket, undefined, 4008, "Protocol version is required");
          return upgradeResponse;
        }
        if (protocol === undefined) {
          yield* rejectConnection(socket, undefined, 4007, "Unsupported protocol version");
          return upgradeResponse;
        }
        const shardName = request.headers["x-durable-pusher-connection-shard"];
        if (shardName === undefined || shardName.length === 0) {
          yield* rejectConnection(socket, protocol, 4000, "Connection routing failed");
          return upgradeResponse;
        }

        const connected = yield* connect(socket, protocol, shardName).pipe(Effect.result);
        if (Result.isFailure(connected)) {
          yield* logNativeError("fetch", connected.failure);
          yield* sendNative(socket, pusherError(connected.failure.message), "fetch");
          yield* closeNative(socket, 1011, "Connection failed", "fetch");
          return upgradeResponse;
        }

        const expectedProtocol = `pusher-channels-protocol-${protocol}`;
        const requestedProtocols = request.headers["sec-websocket-protocol"]
          ?.split(",")
          .map((value) => value.trim());
        return requestedProtocols?.includes(expectedProtocol) === true
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

        const handled = yield* dispatchMessage(socket, message).pipe(Effect.result);

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
          const cleaned = yield* cleanup(decoded.success).pipe(Effect.result);
          if (Result.isFailure(cleaned)) {
            yield* logNativeError("webSocketClose", cleaned.failure);
          }
        }
        yield* socket.close(code, reason);
      });

      const deliver = Effect.fn("ConnectionShard.deliver")(function* (delivery: Delivery) {
        const decoded = yield* Schema.decodeEffect(Delivery)(delivery);
        const rows = yield* db
          .select({
            socketId: subscriptions.socketId,
            startSequence: subscriptions.startSequence,
            state: subscriptions.state,
          })
          .from(subscriptions)
          .where(
            and(
              eq(subscriptions.channel, decoded.channel),
              lt(subscriptions.lastSequence, decoded.sequence),
            ),
          );

        for (const row of rows) {
          if (row.startSequence > decoded.sequence || row.socketId === decoded.excludedSocketId) {
            continue;
          }
          if (row.state === "joining") {
            yield* db
              .insert(pendingEvents)
              .values({
                channel: decoded.channel,
                data: decoded.data,
                event: decoded.event,
                sequence: decoded.sequence,
                socketId: row.socketId,
                userId: decoded.userId ?? null,
              })
              .onConflictDoNothing();
            continue;
          }

          const socket = yield* socketFor(row.socketId);
          if (socket === undefined) {
            continue;
          }
          yield* send(socket, {
            channel: decoded.channel,
            data: decoded.data,
            event: decoded.event,
            ...(decoded.userId === undefined ? {} : { user_id: decoded.userId }),
          });
          yield* db
            .update(subscriptions)
            .set({ lastSequence: decoded.sequence })
            .where(
              and(
                eq(subscriptions.socketId, row.socketId),
                eq(subscriptions.channel, decoded.channel),
                lt(subscriptions.lastSequence, decoded.sequence),
              ),
            );
        }
        return yield* channelSubscriptionCount(decoded.channel);
      }, rpcError("deliver"));

      const terminateUser = Effect.fn("ConnectionShard.terminateUser")(function* (userId: string) {
        const rows = yield* db
          .select({ socketId: connections.socketId })
          .from(connections)
          .where(eq(connections.userId, userId));
        for (const row of rows) {
          const socket = yield* socketFor(row.socketId);
          if (socket !== undefined) {
            yield* socket.close(4009, "Connection terminated");
          }
        }
        return rows.length;
      }, rpcError("terminateUser"));

      const api = { deliver, terminateUser } satisfies ConnectionShardApi;

      return {
        ...api,
        fetch: fetch(),
        webSocketClose,
        webSocketMessage,
      };
    });
  }),
);
