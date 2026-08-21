import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import {
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShard,
  PusherWorker,
  UserShard,
} from "./actors/contracts.ts";
import { HttpActorDependencies } from "./actors/dependencies.ts";
import { AppConfig, AppConfigLive } from "./config.ts";
import { WorkerNames, WorkerNamesLive } from "./hosts/names.ts";
import { makePusherHttp } from "./pusher/http.ts";
import { randomConnectionShardName } from "./sharding.ts";

export { PusherWorker };

const HealthResponse = Schema.Struct({
  app_id: Schema.String,
  effect: Schema.String,
  service: Schema.String,
  status: Schema.String,
});

const encodeHealthResponse = HttpServerResponse.schemaJson(HealthResponse);
const RESPONSE_HEADERS = {
  "access-control-allow-origin": "*",
  "cache-control": "no-store",
};

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
    const connections = yield* ConnectionShard.from(names.connection);
    const channels = yield* ChannelShard.from(names.channel);
    const directories = yield* ChannelDirectoryShard.from(names.directory);
    const users = yield* UserShard.from(names.user);
    const httpDependencies = Layer.mergeAll(
      Layer.succeed(HttpActorDependencies, { channels, directories, users }),
      AppConfigLive,
    );
    const http = yield* makePusherHttp.pipe(Effect.provide(httpDependencies));
    const config = yield* AppConfig;

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
            app_id: config.appId,
            effect: "4.0.0-rc.111",
            service: "durable-pusher",
            status: "ok",
          },
          { headers: RESPONSE_HEADERS },
        );
      }

      if (
        request.method === "GET" &&
        request.headers.upgrade?.toLowerCase() === "websocket" &&
        /\/app\/[^/]+$/.test(pathname)
      ) {
        const shardName = yield* randomConnectionShardName(config.appId);
        const source = request.source;
        if (!(source instanceof Request)) {
          return HttpServerResponse.text("WebSocket request source is unavailable", {
            status: 500,
          });
        }
        const routedRequest = yield* Effect.sync(() =>
          HttpServerRequest.fromWeb(
            new Request(source, {
              headers: Headers.set(
                request.headers,
                "x-durable-pusher-connection-shard",
                shardName,
              ),
            }),
          ),
        );
        return yield* connections.getByName(shardName).fetch(routedRequest);
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
