import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ApplicationPlacement } from "../apps/model.ts";

export const CHANNEL_NAME_PATTERN = /^[A-Za-z0-9_\-=@,.;]{1,164}$/;
export const SOCKET_ID_PATTERN = /^\d+\.\d+$/;
export const MAX_EVENT_NAME_LENGTH = 200;
export const MAX_EVENT_DATA_BYTES = 10 * 1024;

export const ChannelName = Schema.String.check(
  Schema.isLengthBetween(1, 164),
  Schema.isPattern(CHANNEL_NAME_PATTERN),
);
export type ChannelName = typeof ChannelName.Type;

export const SocketId = Schema.String.check(Schema.isPattern(SOCKET_ID_PATTERN));
export type SocketId = typeof SocketId.Type;

export const EventName = Schema.String.check(Schema.isLengthBetween(1, MAX_EVENT_NAME_LENGTH));
export type EventName = typeof EventName.Type;

export const JsonString = Schema.fromJsonString(Schema.Json);

export class ProtocolError extends Schema.TaggedError<ProtocolError>()("ProtocolError", {
  message: Schema.String,
}) {}

export class ApiError extends Schema.TaggedError<ApiError>()("ApiError", {
  message: Schema.String,
  status: Schema.Int,
}) {}

export class ActorError extends Schema.TaggedError<ActorError>()("ActorError", {
  actor: Schema.String,
  message: Schema.String,
  operation: Schema.String,
}) {}

const SubscribeData = Schema.Struct({
  auth: Schema.optionalKey(Schema.String),
  channel: Schema.String,
  channel_data: Schema.optionalKey(Schema.String),
});

const UnsubscribeData = Schema.Struct({ channel: Schema.String });

const SigninData = Schema.Struct({
  auth: Schema.String,
  user_data: Schema.String,
});

const PresenceData = Schema.Struct({
  user_id: Schema.Union([Schema.String, Schema.Finite]),
  user_info: Schema.optionalKey(Schema.Json),
});

const UserData = Schema.Struct({
  id: Schema.String,
  user_info: Schema.optionalKey(Schema.Json),
  watchlist: Schema.optionalKey(Schema.Array(Schema.String)),
});

export const ClientFrame = Schema.Struct({
  channel: Schema.optionalKey(Schema.String),
  data: Schema.optionalKey(Schema.Json),
  event: Schema.String,
});

export const ServerEvent = Schema.Struct({
  channel: Schema.optionalKey(Schema.String),
  data: Schema.optionalKey(Schema.Json),
  event: Schema.String,
  user_id: Schema.optionalKey(Schema.String),
});

export const PublishedEvent = Schema.Struct({
  channel: Schema.String,
  data: Schema.String,
  event: Schema.String,
  excludedSocketId: Schema.optionalKey(Schema.String),
  userId: Schema.optionalKey(Schema.String),
});

export const Delivery = Schema.Struct({
  ...PublishedEvent.fields,
  ...ApplicationPlacement.fields,
  incarnation: Schema.Int,
  sequence: Schema.Int,
});

export const CachedEvent = Schema.Struct({
  data: Schema.String,
  event: Schema.String,
  sequence: Schema.Int,
});

export const ChannelSnapshot = Schema.Struct({
  barrier: Schema.Int,
  cache: Schema.NullOr(CachedEvent),
  incarnation: Schema.Int,
});

export const PresenceMember = Schema.Struct({
  userId: Schema.String,
  userInfo: Schema.Json,
});

export const PresenceConnection = Schema.Struct({
  ...PresenceMember.fields,
  socketId: Schema.String,
});

export const PresenceSnapshot = Schema.Struct({
  barrier: Schema.Int,
  incarnation: Schema.Int,
  members: Schema.Array(PresenceMember),
});

export const PresenceJoin = Schema.Struct({
  ...ApplicationPlacement.fields,
  channel: Schema.String,
  gatewayName: Schema.String,
  socketId: Schema.String,
  userId: Schema.String,
  userInfo: Schema.Json,
});

export const ChannelInfo = Schema.Struct({
  cache: Schema.NullOr(CachedEvent),
  occupied: Schema.Boolean,
  subscriptionCount: Schema.Int,
  userCount: Schema.Int,
});

export const DirectoryEntry = Schema.Struct({
  channel: Schema.String,
  subscriptionCount: Schema.Int,
  userCount: Schema.Int,
});

export const Attachment = Schema.Struct({
  ...ApplicationPlacement.fields,
  eventCount: Schema.Int,
  eventWindow: Schema.Int,
  protocol: Schema.Int,
  shardName: Schema.String,
  socketId: Schema.String,
  subscriptions: Schema.Array(
    Schema.Struct({
      channel: Schema.String,
      kind: Schema.Literals(["public", "private", "presence", "encrypted"]),
      registrationToken: Schema.String,
      state: Schema.Literals(["joining", "active"]),
      userId: Schema.optionalKey(Schema.String),
      userInfo: Schema.optionalKey(Schema.Json),
    }),
  ),
  userData: Schema.optionalKey(Schema.String),
  userId: Schema.optionalKey(Schema.String),
});

export type JsonValue = Schema.Json;
export type ClientFrame = typeof ClientFrame.Type;
export type ServerEvent = typeof ServerEvent.Type;
export type PublishedEvent = typeof PublishedEvent.Type;
export type Delivery = typeof Delivery.Type;
export type DeliveryEncoded = typeof Delivery.Encoded;
export type CachedEvent = typeof CachedEvent.Type;
export type ChannelSnapshot = typeof ChannelSnapshot.Type;
export type PresenceMember = typeof PresenceMember.Type;
export type PresenceConnection = typeof PresenceConnection.Type;
export type PresenceSnapshot = typeof PresenceSnapshot.Type;
export type PresenceJoin = typeof PresenceJoin.Type;
export type PresenceJoinEncoded = typeof PresenceJoin.Encoded;
export type ChannelInfo = typeof ChannelInfo.Type;
export type DirectoryEntry = typeof DirectoryEntry.Type;
export type Attachment = typeof Attachment.Type;
export type SubscribeData = typeof SubscribeData.Type;
export type UserData = typeof UserData.Type;

export type ChannelKind = "public" | "private" | "presence" | "encrypted";

export interface ChannelType {
  readonly kind: ChannelKind;
  readonly cache: boolean;
}

const decodeClientFrameJson = Schema.decodeEffect(Schema.fromJsonString(ClientFrame));
const decodePresenceDataJson = Schema.decodeEffect(Schema.fromJsonString(PresenceData));
const decodeUserDataJson = Schema.decodeEffect(Schema.fromJsonString(UserData));

const protocolError = (message: string) => ProtocolError.make({ message });

export const parseClientFrame = Effect.fn("Pusher.parseClientFrame")((text: string) =>
  decodeClientFrameJson(text).pipe(
    Effect.mapError(() => protocolError("Message does not match the Pusher protocol")),
  ),
);

export const decodeSubscribeData = Effect.fn("Pusher.decodeSubscribeData")(
  (data: JsonValue | undefined) =>
    Schema.decodeUnknownEffect(SubscribeData)(data).pipe(
      Effect.mapError(() => protocolError("Invalid subscription data")),
    ),
);

export const decodeUnsubscribeData = Effect.fn("Pusher.decodeUnsubscribeData")(
  (data: JsonValue | undefined) =>
    Schema.decodeUnknownEffect(UnsubscribeData)(data).pipe(
      Effect.mapError(() => protocolError("Invalid unsubscription data")),
    ),
);

export const decodeSigninData = Effect.fn("Pusher.decodeSigninData")(
  (data: JsonValue | undefined) =>
    Schema.decodeUnknownEffect(SigninData)(data).pipe(
      Effect.mapError(() => protocolError("Invalid sign-in data")),
    ),
);

export const decodePresenceData = Effect.fn("Pusher.decodePresenceData")((data: string) =>
  decodePresenceDataJson(data).pipe(
    Effect.map((presence) => ({
      userId: String(presence.user_id),
      userInfo: presence.user_info ?? null,
    })),
    Effect.mapError(() => protocolError("Presence channel_data is invalid")),
  ),
);

export const decodeUserData = Effect.fn("Pusher.decodeUserData")((data: string) =>
  decodeUserDataJson(data).pipe(
    Effect.filterOrFail(
      (user) => user.id.length > 0,
      () => protocolError("User id cannot be empty"),
    ),
    Effect.mapError(() => protocolError("User data is invalid")),
  ),
);

export const encodeJson = Schema.encodeEffect(JsonString);
export const decodeJson = Schema.decodeEffect(JsonString);
export const encodeServerEvent = Schema.encodeEffect(Schema.fromJsonString(ServerEvent));

export const classifyChannel = (channel: string): ChannelType => {
  if (channel.startsWith("private-encrypted-cache-")) {
    return { cache: true, kind: "encrypted" };
  }
  if (channel.startsWith("private-encrypted-")) {
    return { cache: false, kind: "encrypted" };
  }
  if (channel.startsWith("presence-cache-")) {
    return { cache: true, kind: "presence" };
  }
  if (channel.startsWith("presence-")) {
    return { cache: false, kind: "presence" };
  }
  if (channel.startsWith("private-cache-")) {
    return { cache: true, kind: "private" };
  }
  if (channel.startsWith("private-")) {
    return { cache: false, kind: "private" };
  }
  return { cache: channel.startsWith("cache-"), kind: "public" };
};

export const isValidChannelName = Schema.is(ChannelName);
export const isValidSocketId = Schema.is(SocketId);
export const isServerToUserChannel = (channel: string): boolean =>
  channel.startsWith("#server-to-user-") && channel.length <= 164 && channel.length > 16;

export const isValidEventName = (event: string): boolean =>
  Schema.is(EventName)(event) &&
  !event.startsWith("pusher:") &&
  !event.startsWith("pusher_internal:");

export const toEventData = Effect.fn("Pusher.toEventData")((data: JsonValue) =>
  typeof data === "string" ? Effect.succeed(data) : encodeJson(data),
);

export const eventDataSize = (data: string): number => new TextEncoder().encode(data).byteLength;

export const connectionEstablished = (
  socketId: string,
  activityTimeout: number,
): Effect.Effect<ServerEvent, Schema.SchemaError> =>
  encodeJson({ activity_timeout: activityTimeout, socket_id: socketId }).pipe(
    Effect.map((data) => ({ data, event: "pusher:connection_established" })),
  );

export const subscriptionSucceeded = (channel: string, data = "{}"): ServerEvent => ({
  channel,
  data,
  event: "pusher_internal:subscription_succeeded",
});

export const pusherError = (message: string, code?: number): ServerEvent => ({
  data: code === undefined ? { message } : { code, message },
  event: "pusher:error",
});

export const mapActorError =
  (actor: string, operation: string) =>
  (error: { readonly message?: string } | string): ActorError =>
    ActorError.make({
      actor,
      message: typeof error === "string" ? error : (error.message ?? "Actor operation failed"),
      operation,
    });
