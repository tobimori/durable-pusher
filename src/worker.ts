import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import {
  AppRegistry,
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShard,
  PusherWorker,
  UserShard,
} from "./actors/contracts.ts";
import { HttpActorDependencies } from "./actors/dependencies.ts";
import { makePlacedNamespace } from "./actors/placement.ts";
import {
  ApplicationBootstrap,
  type ApplicationPlacement,
  RuntimeApplication,
} from "./apps/model.ts";
import { AppConfig, AppConfigLive } from "./config.ts";
import { WorkerNames, WorkerNamesLive } from "./hosts/names.ts";
import { makePusherHttp } from "./pusher/http.ts";
import { ApiError } from "./pusher/protocol.ts";
import { randomConnectionShardName } from "./sharding.ts";

export { PusherWorker };

const HealthResponse = Schema.Struct({
  effect: Schema.String,
  service: Schema.String,
  status: Schema.String,
});

const encodeHealthResponse = HttpServerResponse.schemaJson(HealthResponse);
const RESPONSE_HEADERS = {
  "access-control-allow-origin": "*",
  "cache-control": "no-store",
};

const apiError = (status: number, message: string): ApiError => ApiError.make({ message, status });

const actorApiError = Effect.mapError((error: { readonly message: string }) =>
  apiError(503, error.message),
);

const placementOf = (application: RuntimeApplication): ApplicationPlacement => ({
  appId: application.appId,
  jurisdiction: application.jurisdiction,
  locationHint: application.locationHint,
});

export const PusherWorkerLive = PusherWorker.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return {
      name: names.public,
      main: import.meta.url,
      compatibility: {
        date: "2026-07-11",
        flags: ["nodejs_compat"],
      },
      observability: {
        enabled: true,
        logs: {
          enabled: true,
          invocationLogs: true,
          persist: true,
        },
        traces: {
          enabled: true,
          headSamplingRate: 1,
          persist: true,
        },
      },
    };
  }),
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    const environment = yield* Cloudflare.WorkerEnvironment;
    const applications = yield* AppRegistry.from(names.registry);
    const connections = yield* ConnectionShard.from(names.connection);
    const placedConnections = makePlacedNamespace("ConnectionShard", connections, environment);
    const channels = yield* ChannelShard.from(names.channel);
    const directories = yield* ChannelDirectoryShard.from(names.directory);
    const users = yield* UserShard.from(names.user);
    const httpDependencies = Layer.succeed(HttpActorDependencies, {
      applications,
      channels: makePlacedNamespace("ChannelShard", channels, environment),
      directories: makePlacedNamespace("ChannelDirectoryShard", directories, environment),
      users: makePlacedNamespace("UserShard", users, environment),
    });
    const http = yield* makePusherHttp.pipe(Effect.provide(httpDependencies));
    const config = yield* AppConfig;
    const bootstrapInput = yield* Schema.encodeEffect(ApplicationBootstrap)({
      appId: config.appId,
      appKey: config.appKey,
      appSecret: Redacted.value(config.appSecret),
      authToken: Redacted.value(config.authToken),
      encryptionMasterKey: Redacted.value(config.encryptionMasterKey),
      jurisdiction: Option.none(),
      locationHint: Option.none(),
      name: "Bootstrap application",
    }).pipe(Effect.orDie);
    const bootstrapApplication = Effect.fn("PusherWorker.bootstrapApplication")(function* () {
      yield* applications.getByName("applications").bootstrap(bootstrapInput);
    }, actorApiError);

    const route = Effect.fn("PusherWorker.route")(function* (
      request: HttpServerRequest.HttpServerRequest,
      pathname: string,
    ) {
      if (request.method === "OPTIONS") {
        return HttpServerResponse.empty({
          headers: {
            "access-control-allow-headers": "Authorization, Content-Type, X-Pusher-Library",
            "access-control-allow-methods": "GET, POST, OPTIONS",
            "access-control-allow-origin": "*",
            "cache-control": "no-store",
          },
          status: 204,
        });
      }
      if (request.method === "GET" && pathname === "/health") {
        return yield* encodeHealthResponse(
          {
            effect: "4.0.0-rc.111",
            service: "durable-pusher",
            status: "ok",
          },
          { headers: RESPONSE_HEADERS },
        );
      }
      yield* bootstrapApplication();

      if (
        request.method === "GET" &&
        request.headers.upgrade?.toLowerCase() === "websocket" &&
        /\/app\/[^/]+$/.test(pathname)
      ) {
        const keyResult = yield* Schema.decodeEffect(Schema.StringFromUriComponent)(
          pathname.slice(5),
        ).pipe(Effect.result);
        const encodedApplication = Result.isSuccess(keyResult)
          ? Option.fromNullishOr(
              yield* applications
                .getByName("applications")
                .resolveByKey(keyResult.success)
                .pipe(actorApiError),
            )
          : Option.none();
        const application = Option.isSome(encodedApplication)
          ? Option.some(
              yield* Schema.decodeEffect(RuntimeApplication)(encodedApplication.value).pipe(
                actorApiError,
              ),
            )
          : Option.none();
        const shardName = Option.isSome(application)
          ? yield* randomConnectionShardName(application.value.appId)
          : "invalid:connection:0";
        const source = request.source;
        if (!(source instanceof Request)) {
          return HttpServerResponse.text("WebSocket request source is unavailable", {
            status: 500,
          });
        }
        const routedRequest = yield* Effect.sync(() =>
          HttpServerRequest.fromWeb(
            new Request(source, {
              headers: Headers.set(request.headers, "x-durable-pusher-connection-shard", shardName),
            }),
          ),
        );
        const connection = Option.isSome(application)
          ? yield* placedConnections
              .getByName(shardName, placementOf(application.value))
              .pipe(actorApiError)
          : connections.getByName(shardName);
        return yield* connection.fetch(routedRequest).pipe(actorApiError);
      }
      return yield* http.handle(request);
    });

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const url = yield* Schema.decodeEffect(Schema.URLFromString)(request.originalUrl);
        const response = yield* route(request, url.pathname).pipe(
          Effect.catchTag("ApiError", (error) =>
            Effect.gen(function* () {
              yield* Effect.logWarning("Pusher API request rejected").pipe(
                Effect.annotateLogs({ message: error.message, status: error.status }),
              );
              return yield* http.errorResponse(error);
            }),
          ),
          Effect.tap((response) =>
            Effect.logInfo("Pusher request completed").pipe(
              Effect.annotateLogs({
                method: request.method,
                path: url.pathname,
                status: response.status,
              }),
            ),
          ),
          Effect.withSpan("PusherWorker.fetch", {
            attributes: {
              "http.request.method": request.method,
              "url.path": url.pathname,
            },
          }),
        );
        return response;
      }),
    };
  }).pipe(Effect.provide(AppConfigLive)),
).pipe(Layer.provide(WorkerNamesLive));

export default PusherWorkerLive;
