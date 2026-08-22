import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as UrlParams from "effect/unstable/http/UrlParams";
import { HttpActorDependencies } from "../actors/dependencies.ts";
import {
  AppKey,
  ApplicationPlacement,
  ResolvedApplication,
  RuntimeApplication,
} from "../apps/model.ts";
import {
  DIRECTORY_SHARD_COUNT,
  channelShardName,
  connectionShardCatalogName,
  connectionShardName,
  fanoutRelayName,
} from "../sharding.ts";
import {
  channelAuthorization,
  encryptedChannelSharedSecret,
  hmacSha256Hex,
  md5Hex,
  timingSafeEqual,
  userAuthentication,
} from "./crypto.ts";
import {
  ApiError,
  MAX_EVENT_DATA_BYTES,
  SOCKET_ID_PATTERN,
  classifyChannel,
  eventDataSize,
  isServerToUserChannel,
  isValidChannelName,
  isValidEventName,
  type ChannelInfo,
  type JsonValue,
} from "./protocol.ts";

type JsonObject = { [key: string]: JsonValue };
type ParsedUrl = typeof Schema.URLFromString.Type;

const SingleEvent = Schema.Struct({
  channel: Schema.optionalKey(Schema.String),
  channels: Schema.optionalKey(Schema.Array(Schema.String)),
  data: Schema.String,
  info: Schema.optionalKey(Schema.String),
  name: Schema.String,
  socket_id: Schema.optionalKey(Schema.String),
});

const BatchEvent = Schema.Struct({
  channel: Schema.String,
  data: Schema.String,
  info: Schema.optionalKey(Schema.String),
  name: Schema.String,
  socket_id: Schema.optionalKey(Schema.String),
});

const Batch = Schema.Struct({
  batch: Schema.Array(BatchEvent),
});

const ChannelAuthorizationForm = Schema.Struct({
  channel_data: Schema.optionalKey(Schema.String),
  channel_name: Schema.String,
  socket_id: Schema.String,
  user_id: Schema.optionalKey(Schema.String),
  user_info: Schema.optionalKey(Schema.String),
});

const UserAuthorizationForm = Schema.Struct({
  socket_id: Schema.String,
  user_data: Schema.String,
});

const PresenceChannelData = Schema.Struct({
  user_id: Schema.Union([Schema.String, Schema.Finite]),
  user_info: Schema.optionalKey(Schema.Json),
});

const PresenceChannelDataJson = Schema.fromJsonString(PresenceChannelData);
const EncryptionMasterKey = Schema.Uint8ArrayFromBase64.pipe(
  Schema.check(Schema.isLengthBetween(32, 32)),
);
const RestTimestamp = Schema.FiniteFromString.pipe(Schema.check(Schema.isInt()));

const decodeUrl = Schema.decodeUnknownEffect(Schema.URLFromString);
const decodeUriComponent = Schema.decodeUnknownEffect(Schema.StringFromUriComponent);
const decodeJsonValue = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));
const decodeFormRecord = Schema.decodeEffect(UrlParams.schemaRecord);
const decodeChannelAuthorizationForm = Schema.decodeUnknownEffect(ChannelAuthorizationForm);
const decodeUserAuthorizationForm = Schema.decodeUnknownEffect(UserAuthorizationForm);
const decodePresenceChannelData = Schema.decodeEffect(PresenceChannelDataJson);
const encodePresenceChannelData = Schema.encodeEffect(PresenceChannelDataJson);
const decodeEncryptionMasterKey = Schema.decodeEffect(EncryptionMasterKey);
const decodeRestTimestamp = Schema.decodeEffect(RestTimestamp);
const encodeJsonResponse = HttpServerResponse.schemaJson(Schema.Json);

const RESPONSE_HEADERS = {
  "access-control-allow-origin": "*",
  "cache-control": "no-store",
};

const apiError = (status: number, message: string): ApiError => ApiError.make({ message, status });

const json = (value: JsonValue, status = 200) =>
  encodeJsonResponse(value, {
    headers: RESPONSE_HEADERS,
    status,
  });

export const apiErrorResponse = Effect.fn("PusherHttp.apiErrorResponse")(function* (
  error: ApiError,
) {
  return yield* json({ error: error.message }, error.status);
});

const required = Effect.fn("PusherHttp.required")(function* (
  value: Option.Option<string>,
  message: string,
) {
  if (Option.isNone(value)) {
    return yield* apiError(400, message);
  }
  return value.value;
});

const placementOf = (application: RuntimeApplication): ApplicationPlacement => ({
  appId: application.appId,
  jurisdiction: application.jurisdiction,
  locationHint: application.locationHint,
});

const actorApiError = Effect.mapError((error: { readonly message: string }) =>
  apiError(503, error.message),
);

const decodeJsonBody = Effect.fn("PusherHttp.decodeJsonBody")(function* <
  S extends Schema.Constraint,
>(schema: S, body: Uint8Array) {
  const text = yield* Effect.sync(() => new TextDecoder().decode(body));
  const value = yield* decodeJsonValue(text).pipe(
    Effect.mapError(() => apiError(400, "Request body is not valid JSON")),
  );
  return yield* Schema.decodeEffect(schema)(value).pipe(
    Effect.mapError(() => apiError(400, "Request body does not match the Pusher API")),
  );
});

const decodePathSegment = Effect.fn("PusherHttp.decodePathSegment")(function* (value: string) {
  return yield* decodeUriComponent(value).pipe(
    Effect.mapError(() => apiError(400, "Path contains invalid percent encoding")),
  );
});

const readFormRecord = Effect.fn("PusherHttp.readFormRecord")(function* (
  request: HttpServerRequest.HttpServerRequest,
) {
  const params = yield* request.urlParamsBody;
  return yield* decodeFormRecord(params).pipe(
    Effect.mapError(() => apiError(400, "Request body is not valid form data")),
  );
});

const validPublishedChannel = (channel: string): boolean =>
  isValidChannelName(channel) || isServerToUserChannel(channel);

const infoAttributes = (info: string | undefined, channelInfo: ChannelInfo): JsonObject => {
  if (info === undefined || info.length === 0) {
    return {};
  }
  const fields = new Set(info.split(","));
  return {
    ...(fields.has("subscription_count")
      ? { subscription_count: channelInfo.subscriptionCount }
      : {}),
    ...(fields.has("user_count") ? { user_count: channelInfo.userCount } : {}),
  };
};

const canonicalQuery = Effect.fn("PusherHttp.canonicalQuery")(function* (url: ParsedUrl) {
  const entries = Array.from(url.searchParams.entries()).filter(
    ([key]) => key.toLowerCase() !== "auth_signature",
  );
  const seen = new Set<string>();
  for (const [key] of entries) {
    const normalized = key.toLowerCase();
    if (seen.has(normalized) && !normalized.endsWith("[]")) {
      return yield* apiError(401, `Duplicate authentication parameter: ${normalized}`);
    }
    seen.add(normalized);
  }
  return entries
    .map(([key, value], index) => ({ index, key: key.toLowerCase(), value }))
    .sort((left, right) =>
      left.key < right.key ? -1 : left.key > right.key ? 1 : left.index - right.index,
    )
    .map(({ key, value }) => `${key}=${value}`)
    .join("&");
});

export const makePusherHttp = Effect.gen(function* () {
  const {
    applications,
    authorities,
    catalogs: connectionShardCatalogs,
    channels: channelShards,
    directories: directoryShards,
    relays: fanoutRelays,
  } = yield* HttpActorDependencies;

  const resolveApplicationByKey = Effect.fn("PusherHttp.resolveApplicationByKey")(function* (
    appKey: string,
  ) {
    const decodedKey = yield* Schema.decodeEffect(AppKey)(appKey).pipe(Effect.result);
    if (Result.isFailure(decodedKey)) {
      return Option.none<RuntimeApplication>();
    }
    const authority = authorities.getByName(decodedKey.success);
    const encoded = Option.fromNullishOr(yield* authority.resolve().pipe(actorApiError));
    return Option.isNone(encoded)
      ? Option.none<RuntimeApplication>()
      : Option.some(
          yield* Schema.decodeEffect(ResolvedApplication)(encoded.value).pipe(actorApiError),
        );
  });

  const authenticateRestRequest = Effect.fn("PusherHttp.authenticateRestRequest")(function* (
    request: HttpServerRequest.HttpServerRequest,
    url: ParsedUrl,
    body: Uint8Array,
  ) {
    const appKey = yield* required(
      Option.fromNullishOr(url.searchParams.get("auth_key")),
      "Missing authentication key",
    );
    const timestampText = yield* required(
      Option.fromNullishOr(url.searchParams.get("auth_timestamp")),
      "Missing authentication timestamp",
    );
    const version = Option.fromNullishOr(url.searchParams.get("auth_version"));
    const signature = yield* required(
      Option.fromNullishOr(url.searchParams.get("auth_signature")),
      "Missing authentication signature",
    );

    const application = yield* resolveApplicationByKey(appKey);
    if (Option.isNone(application)) {
      return yield* apiError(401, "Missing or invalid authentication parameters");
    }
    if (!Option.contains(version, "1.0")) {
      return yield* apiError(401, "Unsupported authentication version");
    }

    const timestamp = yield* decodeRestTimestamp(timestampText).pipe(
      Effect.mapError(() =>
        apiError(401, "Authentication timestamp is outside the accepted window"),
      ),
    );
    const now = yield* Clock.currentTimeMillis;
    if (Math.abs(Math.floor(now / 1_000) - timestamp) >= 600) {
      return yield* apiError(401, "Authentication timestamp is outside the accepted window");
    }

    const bodyMd5 = Option.fromNullishOr(url.searchParams.get("body_md5"));
    if (body.byteLength > 0 && Option.isNone(bodyMd5)) {
      return yield* apiError(401, "body_md5 is required for requests with a body");
    }
    if (Option.isSome(bodyMd5) && !timingSafeEqual(bodyMd5.value, md5Hex(body))) {
      return yield* apiError(401, "body_md5 does not match the request body");
    }

    const stringToSign = `${request.method}\n${url.pathname}\n${yield* canonicalQuery(url)}`;
    if (!timingSafeEqual(signature, hmacSha256Hex(application.value.appSecret, stringToSign))) {
      return yield* apiError(401, "Invalid authentication signature");
    }
    return application.value;
  });

  const publishOne = Effect.fn("PusherHttp.publishOne")(function* (
    application: RuntimeApplication,
    event: typeof BatchEvent.Type,
  ) {
    if (!validPublishedChannel(event.channel)) {
      return yield* apiError(400, `Invalid channel name: ${event.channel}`);
    }
    if (!isValidEventName(event.name)) {
      return yield* apiError(400, `Invalid event name: ${event.name}`);
    }
    if (eventDataSize(event.data) > MAX_EVENT_DATA_BYTES) {
      return yield* apiError(413, "Event data exceeds 10 KB");
    }
    if (event.socket_id !== undefined && !SOCKET_ID_PATTERN.test(event.socket_id)) {
      return yield* apiError(400, "Invalid socket_id");
    }

    const placement = placementOf(application);
    const encodedPlacement =
      yield* Schema.encodeEffect(ApplicationPlacement)(placement).pipe(actorApiError);
    return yield* Effect.gen(function* () {
      const channel = yield* channelShards.getByName(
        channelShardName(application.appId, event.channel),
        placement,
      );
      const requestedInfo = new Set(event.info?.split(",") ?? []);
      const info =
        requestedInfo.has("subscription_count") || requestedInfo.has("user_count")
          ? yield* channel.info(encodedPlacement, event.channel)
          : undefined;
      yield* channel.publish(
        encodedPlacement,
        event.channel,
        event.name,
        event.data,
        event.socket_id ?? null,
        null,
        true,
      );
      return info === undefined ? {} : infoAttributes(event.info, info);
    }).pipe(actorApiError);
  });

  const postEvents = Effect.fn("PusherHttp.postEvents")(function* (
    application: RuntimeApplication,
    body: Uint8Array,
  ) {
    const event = yield* decodeJsonBody(SingleEvent, body);
    if ((event.channel === undefined) === (event.channels === undefined)) {
      return yield* apiError(400, "Supply exactly one of channel or channels");
    }
    const channels = event.channel === undefined ? [...(event.channels ?? [])] : [event.channel];
    if (channels.length === 0 || channels.length > 100) {
      return yield* apiError(400, "An event must target between 1 and 100 channels");
    }
    if (
      channels.some((channel) => classifyChannel(channel).kind === "encrypted") &&
      channels.length > 1
    ) {
      return yield* apiError(400, "Encrypted events can target only one channel");
    }

    const results = yield* Effect.forEach(
      channels,
      (channel) =>
        publishOne(application, {
          channel,
          data: event.data,
          name: event.name,
          ...(event.info === undefined ? {} : { info: event.info }),
          ...(event.socket_id === undefined ? {} : { socket_id: event.socket_id }),
        }),
      { concurrency: 1 },
    );
    if (event.info === undefined) {
      return yield* json({});
    }
    return yield* json({
      channels: Object.fromEntries(
        channels.map((channel, index) => [channel, results[index] ?? {}]),
      ),
    });
  });

  const postBatch = Effect.fn("PusherHttp.postBatch")(function* (
    application: RuntimeApplication,
    body: Uint8Array,
  ) {
    const request = yield* decodeJsonBody(Batch, body);
    if (request.batch.length === 0 || request.batch.length > 10) {
      return yield* apiError(400, "A batch must contain between 1 and 10 events");
    }
    const attributes = yield* Effect.forEach(
      request.batch,
      (event) => publishOne(application, event),
      { concurrency: 1 },
    );
    if (request.batch.some((event) => event.info !== undefined)) {
      return yield* json({ batch: attributes });
    }
    return yield* json({});
  });

  const listChannels = Effect.fn("PusherHttp.listChannels")(function* (
    application: RuntimeApplication,
    url: ParsedUrl,
  ) {
    const prefix = url.searchParams.get("filter_by_prefix");
    const info = url.searchParams.get("info") ?? undefined;
    const fields = new Set(info?.split(",") ?? []);
    if (fields.has("user_count") && prefix?.startsWith("presence-") !== true) {
      return yield* apiError(400, "user_count requires a presence channel prefix");
    }

    const results = yield* Effect.forEach(
      Array.from({ length: DIRECTORY_SHARD_COUNT }, (_, shard) => shard),
      Effect.fn("PusherHttp.listChannels.shard")(function* (shard) {
        const directory = yield* directoryShards.getByName(
          `${application.appId}:directory:${shard}`,
          placementOf(application),
        );
        return yield* directory.list(prefix);
      }),
      { concurrency: 8 },
    ).pipe(actorApiError);
    const entries = results
      .flat()
      .sort((left, right) =>
        left.channel < right.channel ? -1 : left.channel > right.channel ? 1 : 0,
      );
    const resolvedEntries =
      fields.size === 0
        ? entries
        : yield* Effect.forEach(
            entries,
            Effect.fn("PusherHttp.listChannels.info")(function* (entry) {
              const placement = placementOf(application);
              const channel = yield* channelShards.getByName(
                channelShardName(application.appId, entry.channel),
                placement,
              );
              const info = yield* channel.info(
                yield* Schema.encodeEffect(ApplicationPlacement)(placement),
                entry.channel,
              );
              return {
                channel: entry.channel,
                subscriptionCount: info.subscriptionCount,
                userCount: info.userCount,
              };
            }),
            { concurrency: 8 },
          ).pipe(actorApiError);
    const channels = Object.fromEntries(
      resolvedEntries.map((entry) => [
        entry.channel,
        {
          ...(fields.has("subscription_count")
            ? { subscription_count: entry.subscriptionCount }
            : {}),
          ...(fields.has("user_count") ? { user_count: entry.userCount } : {}),
        },
      ]),
    );
    return yield* json({ channels });
  });

  const getChannelInfo = Effect.fn("PusherHttp.getChannelInfo")(function* (
    application: RuntimeApplication,
    channel: string,
    url: ParsedUrl,
  ) {
    if (!validPublishedChannel(channel)) {
      return yield* apiError(400, "Invalid channel name");
    }
    const fields = url.searchParams.get("info") ?? undefined;
    const requestedFields = new Set(fields?.split(",") ?? []);
    const channelType = classifyChannel(channel);
    if (requestedFields.has("user_count") && channelType.kind !== "presence") {
      return yield* apiError(400, "user_count is available only for presence channels");
    }
    if (requestedFields.has("subscription_count") && channelType.kind === "presence") {
      return yield* apiError(400, "subscription_count is unavailable for presence channels");
    }
    const placement = placementOf(application);
    const encodedPlacement =
      yield* Schema.encodeEffect(ApplicationPlacement)(placement).pipe(actorApiError);
    const info = yield* Effect.gen(function* () {
      const channelShard = yield* channelShards.getByName(
        channelShardName(application.appId, channel),
        placement,
      );
      return yield* channelShard.info(encodedPlacement, channel);
    }).pipe(actorApiError);
    return yield* json({ occupied: info.occupied, ...infoAttributes(fields, info) });
  });

  const getPresenceUsers = Effect.fn("PusherHttp.getPresenceUsers")(function* (
    application: RuntimeApplication,
    channel: string,
  ) {
    if (classifyChannel(channel).kind !== "presence" || !isValidChannelName(channel)) {
      return yield* apiError(400, "Users are available only for presence channels");
    }
    const placement = placementOf(application);
    const encodedPlacement =
      yield* Schema.encodeEffect(ApplicationPlacement)(placement).pipe(actorApiError);
    const users = yield* Effect.gen(function* () {
      const channelShard = yield* channelShards.getByName(
        channelShardName(application.appId, channel),
        placement,
      );
      return yield* channelShard.presenceUsers(encodedPlacement, channel);
    }).pipe(actorApiError);
    return yield* json({ users: users.map((id) => ({ id })) });
  });

  const terminateUser = Effect.fn("PusherHttp.terminateUser")(function* (
    application: RuntimeApplication,
    userId: string,
  ) {
    const placement = placementOf(application);
    const encodedPlacement =
      yield* Schema.encodeEffect(ApplicationPlacement)(placement).pipe(actorApiError);
    yield* Effect.gen(function* () {
      const catalog = yield* connectionShardCatalogs.getByName(
        connectionShardCatalogName(application.appId),
        placement,
      );
      const shardCount = yield* catalog.shardCount(encodedPlacement);
      const path = "terminate.root";
      const relay = yield* fanoutRelays.getByName(
        fanoutRelayName(application.appId, `#server-to-user-${userId}`, path),
        placement,
      );
      yield* relay.terminateUser(
        encodedPlacement,
        userId,
        Array.from({ length: shardCount }, (_, shard) =>
          connectionShardName(application.appId, shard),
        ),
        path,
      );
    }).pipe(actorApiError);
    return yield* json({});
  });

  const handleRestApi = Effect.fn("PusherHttp.handleRestApi")(function* (
    request: HttpServerRequest.HttpServerRequest,
    url: ParsedUrl,
  ) {
    const buffer = yield* request.arrayBuffer;
    const body = yield* Effect.sync(() => new Uint8Array(buffer));
    const application = yield* authenticateRestRequest(request, url, body);

    const legacyTerminateMatch = /^\/users\/([^/]+)\/terminate_connections$/.exec(url.pathname);
    if (request.method === "POST" && legacyTerminateMatch?.[1] !== undefined) {
      return yield* terminateUser(application, yield* decodePathSegment(legacyTerminateMatch[1]));
    }

    const appMatch = /^\/apps\/([^/]+)(\/.*)?$/.exec(url.pathname);
    if (appMatch?.[1] === undefined) {
      return yield* apiError(401, "Unknown app id");
    }
    const appId = yield* decodePathSegment(appMatch[1]);
    if (appId !== application.appId) {
      return yield* apiError(401, "Unknown app id");
    }
    const suffix = appMatch[2] ?? "";

    if (request.method === "POST" && suffix === "/events") {
      return yield* postEvents(application, body);
    }
    if (request.method === "POST" && suffix === "/batch_events") {
      return yield* postBatch(application, body);
    }
    if (request.method === "GET" && suffix === "/channels") {
      return yield* listChannels(application, url);
    }

    const usersMatch = /^\/channels\/([^/]+)\/users$/.exec(suffix);
    if (request.method === "GET" && usersMatch?.[1] !== undefined) {
      return yield* getPresenceUsers(application, yield* decodePathSegment(usersMatch[1]));
    }
    const channelMatch = /^\/channels\/([^/]+)$/.exec(suffix);
    if (request.method === "GET" && channelMatch?.[1] !== undefined) {
      return yield* getChannelInfo(application, yield* decodePathSegment(channelMatch[1]), url);
    }
    const terminateMatch = /^\/users\/([^/]+)\/terminate_connections$/.exec(suffix);
    if (request.method === "POST" && terminateMatch?.[1] !== undefined) {
      return yield* terminateUser(application, yield* decodePathSegment(terminateMatch[1]));
    }
    return yield* apiError(404, "Pusher API endpoint not found");
  });

  const requireAuthorizationToken = Effect.fn("PusherHttp.requireAuthorizationToken")(function* (
    request: HttpServerRequest.HttpServerRequest,
  ) {
    const authorization = Option.fromNullishOr(request.headers.authorization);
    if (
      Option.isNone(authorization) ||
      !authorization.value.startsWith("Bearer ") ||
      authorization.value.length === 7
    ) {
      return yield* apiError(403, "Authorization denied");
    }
    const encodedApplication = Option.fromNullishOr(
      yield* applications
        .getByName("applications")
        .resolveByAuthToken(authorization.value.slice(7))
        .pipe(actorApiError),
    );
    if (Option.isNone(encodedApplication)) {
      return yield* apiError(403, "Authorization denied");
    }
    return yield* Schema.decodeEffect(RuntimeApplication)(encodedApplication.value).pipe(
      actorApiError,
    );
  });

  const handleChannelAuthorization = Effect.fn("PusherHttp.handleChannelAuthorization")(function* (
    request: HttpServerRequest.HttpServerRequest,
  ) {
    const application = yield* requireAuthorizationToken(request);
    const params = yield* decodeChannelAuthorizationForm(yield* readFormRecord(request)).pipe(
      Effect.mapError(() => apiError(400, "Request body does not match the authorization API")),
    );
    if (!SOCKET_ID_PATTERN.test(params.socket_id)) {
      return yield* apiError(400, "Invalid socket_id");
    }
    if (!isValidChannelName(params.channel_name)) {
      return yield* apiError(400, "Invalid channel name");
    }

    const channelType = classifyChannel(params.channel_name);
    let channelData = params.channel_data;
    if (channelData !== undefined) {
      yield* (
        channelType.kind === "presence"
          ? decodePresenceChannelData(channelData)
          : decodeJsonValue(channelData)
      ).pipe(Effect.mapError(() => apiError(400, "channel_data is not valid JSON")));
    } else if (channelType.kind === "presence") {
      if (params.user_id === undefined || params.user_id.length === 0) {
        return yield* apiError(400, "Presence authorization requires user_id or channel_data");
      }
      const userInfo =
        params.user_info === undefined
          ? {}
          : yield* decodeJsonValue(params.user_info).pipe(
              Effect.mapError(() => apiError(400, "user_info is not valid JSON")),
            );
      channelData = yield* encodePresenceChannelData({
        user_id: params.user_id,
        user_info: userInfo,
      }).pipe(Effect.mapError(() => apiError(400, "Presence channel_data could not be encoded")));
    }

    const response: JsonObject = {
      auth: channelAuthorization(
        application.appKey,
        application.appSecret,
        params.socket_id,
        params.channel_name,
        channelData,
      ),
      ...(channelData === undefined ? {} : { channel_data: channelData }),
    };
    if (channelType.kind === "encrypted") {
      const masterKey = yield* decodeEncryptionMasterKey(application.encryptionMasterKey).pipe(
        Effect.mapError(() => apiError(503, "Encrypted channels are not configured")),
      );
      response.shared_secret = yield* encryptedChannelSharedSecret(
        params.channel_name,
        masterKey,
      ).pipe(Effect.mapError(() => apiError(503, "Encrypted channels are not configured")));
    }
    return yield* json(response);
  });

  const handleUserAuthorization = Effect.fn("PusherHttp.handleUserAuthorization")(function* (
    request: HttpServerRequest.HttpServerRequest,
  ) {
    const application = yield* requireAuthorizationToken(request);
    const params = yield* decodeUserAuthorizationForm(yield* readFormRecord(request)).pipe(
      Effect.mapError(() => apiError(400, "Request body does not match the authorization API")),
    );
    if (!SOCKET_ID_PATTERN.test(params.socket_id)) {
      return yield* apiError(400, "Invalid socket_id");
    }
    yield* decodeJsonValue(params.user_data).pipe(
      Effect.mapError(() => apiError(400, "user_data is not valid JSON")),
    );
    return yield* json({
      auth: userAuthentication(
        application.appKey,
        application.appSecret,
        params.socket_id,
        params.user_data,
      ),
      user_data: params.user_data,
    });
  });

  const handle = Effect.fn("PusherHttp.handle")(function* (
    request: HttpServerRequest.HttpServerRequest,
  ) {
    const url = yield* decodeUrl(request.originalUrl).pipe(
      Effect.mapError(() => apiError(400, "Request URL is invalid")),
    );
    if (request.method === "POST" && url.pathname === "/pusher/auth") {
      return yield* handleChannelAuthorization(request);
    }
    if (request.method === "POST" && url.pathname === "/pusher/user-auth") {
      return yield* handleUserAuthorization(request);
    }
    if (url.pathname.startsWith("/apps/") || url.pathname.startsWith("/users/")) {
      return yield* handleRestApi(request, url);
    }
    return yield* json({ error: "Not found" }, 404);
  });

  return { errorResponse: apiErrorResponse, handle };
});
