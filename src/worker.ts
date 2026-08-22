import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Random from "effect/Random";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { ChannelShardLive } from "./actors/channel.ts";
import { ConnectionShardCatalogLive } from "./actors/catalog.ts";
import { ConnectionShardLive } from "./actors/connection.ts";
import {
  AppRegistry,
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShardCatalog,
  ConnectionShard,
  FanoutRelay,
  PusherWorker,
} from "./actors/contracts.ts";
import {
  ChannelActorDependencies,
  ConnectionActorDependencies,
  FanoutRelayDependencies,
  HttpActorDependencies,
} from "./actors/dependencies.ts";
import { ChannelDirectoryShardLive } from "./actors/directory.ts";
import { FanoutRelayLive } from "./actors/relay.ts";
import { makePlacedNamespace } from "./actors/placement.ts";
import { AppRegistryLive } from "./actors/registry.ts";
import {
  ApplicationBootstrap,
  ApplicationPlacement,
  type ApplicationPlacementEncoded,
  RuntimeApplication,
} from "./apps/model.ts";
import { makeApplicationsHttp } from "./apps/http.ts";
import { AppConfig, AppConfigLive } from "./config.ts";
import { WorkerNames, WorkerNamesLive } from "./hosts/names.ts";
import { makePusherHttp } from "./pusher/http.ts";
import { ApiError } from "./pusher/protocol.ts";
import { connectionShardCatalogName, connectionShardName, fanoutRelayName } from "./sharding.ts";

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
      name: names.worker,
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
    const environment = yield* Cloudflare.WorkerEnvironment;
    const applications = yield* AppRegistry.from(PusherWorker);
    const channels = yield* ChannelShard.from(PusherWorker);
    const catalogs = yield* ConnectionShardCatalog.from(PusherWorker);
    const connections = yield* ConnectionShard.from(PusherWorker);
    const directories = yield* ChannelDirectoryShard.from(PusherWorker);
    const relays = yield* FanoutRelay.from(PusherWorker);
    const placedChannels = makePlacedNamespace("ChannelShard", channels, environment);
    const placedConnections = makePlacedNamespace("ConnectionShard", connections, environment);
    const placedCatalogs = makePlacedNamespace("ConnectionShardCatalog", catalogs, environment);
    const placedDirectories = makePlacedNamespace(
      "ChannelDirectoryShard",
      directories,
      environment,
    );
    const placedRelays = makePlacedNamespace("FanoutRelay", relays, environment);
    const config = yield* AppConfig;
    if (config.connectionShardSoftLimit < 1) {
      return yield* Effect.die(new Error("PUSHER_CONNECTION_SHARD_SOFT_LIMIT must be positive"));
    }
    const actorsLive = Layer.mergeAll(
      AppRegistryLive,
      ChannelDirectoryShardLive,
      ChannelShardLive.pipe(
        Layer.provide(
          Layer.succeed(ChannelActorDependencies, {
            connections: placedConnections,
            directories: placedDirectories,
            relays: placedRelays,
          }),
        ),
      ),
      ConnectionShardLive.pipe(
        Layer.provide(
          Layer.succeed(ConnectionActorDependencies, {
            applications,
            channels: placedChannels,
            connectionShardSoftLimit: config.connectionShardSoftLimit,
          }),
        ),
      ),
      ConnectionShardCatalogLive,
      FanoutRelayLive.pipe(
        Layer.provide(
          Layer.succeed(FanoutRelayDependencies, {
            connections: placedConnections,
            relays: placedRelays,
          }),
        ),
      ),
    );
    yield* Effect.all(
      [
        AppRegistry,
        ChannelDirectoryShard,
        ChannelShard,
        ConnectionShardCatalog,
        ConnectionShard,
        FanoutRelay,
      ],
      { concurrency: "unbounded", discard: true },
    ).pipe(Effect.provide(actorsLive));
    const httpDependencies = Layer.succeed(HttpActorDependencies, {
      applications,
      catalogs: placedCatalogs,
      channels: placedChannels,
      directories: placedDirectories,
      relays: placedRelays,
    });
    const http = yield* makePusherHttp.pipe(Effect.provide(httpDependencies));
    const terminateApplication = Effect.fn("PusherWorker.terminateApplication")(function* (
      encodedPlacement: ApplicationPlacementEncoded,
    ) {
      const placement = yield* Schema.decodeEffect(ApplicationPlacement)(encodedPlacement);
      const catalog = yield* placedCatalogs.getByName(
        connectionShardCatalogName(placement.appId),
        placement,
      );
      const shardCount = yield* catalog.shardCount(encodedPlacement);
      const path = "terminate.root";
      const relay = yield* placedRelays.getByName(
        fanoutRelayName(placement.appId, "application-control", path),
        placement,
      );
      return yield* relay.terminateApplication(
        encodedPlacement,
        Array.from({ length: shardCount }, (_, shard) =>
          connectionShardName(placement.appId, shard),
        ),
        path,
      );
    });
    const applicationsHttp = makeApplicationsHttp(
      applications,
      config.controlToken,
      terminateApplication,
    );
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
    const bootstrapApplication = Effect.fn("PusherWorker.bootstrapApplication")(
      function* () {
        yield* applications.getByName("applications").bootstrap(bootstrapInput);
      },
      Effect.mapError(() => apiError(503, "Application registry is unavailable")),
    );
    const shardCounts = new Map<string, number>();

    const routeConnection = Effect.fn("PusherWorker.routeConnection")(function* (
      request: HttpServerRequest.HttpServerRequest,
      source: Request,
      application: RuntimeApplication,
    ) {
      const placement = placementOf(application);
      const encodedPlacement =
        yield* Schema.encodeEffect(ApplicationPlacement)(placement).pipe(actorApiError);
      const catalog = yield* placedCatalogs
        .getByName(connectionShardCatalogName(application.appId), placement)
        .pipe(actorApiError);
      let shardCount = shardCounts.get(application.appId);
      if (shardCount === undefined) {
        shardCount = yield* catalog.shardCount(encodedPlacement).pipe(actorApiError);
        shardCounts.set(application.appId, shardCount);
      }
      let candidateFloor = 0;

      for (let round = 0; round < 32; round += 1) {
        const attempts = Math.min(shardCount, 8);
        const tried = new Set<number>();
        for (let attempt = 0; attempt < attempts; attempt += 1) {
          let shard =
            attempt === 0 && candidateFloor < shardCount
              ? yield* Random.nextIntBetween(candidateFloor, shardCount)
              : yield* Random.nextIntBetween(0, shardCount);
          while (tried.has(shard) && tried.size < shardCount) {
            shard = (shard + 1) % shardCount;
          }
          tried.add(shard);
          const shardName = connectionShardName(application.appId, shard);
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
          const connection = yield* placedConnections
            .getByName(shardName, placement)
            .pipe(actorApiError);
          const response = yield* connection.fetch(routedRequest).pipe(actorApiError);
          if (!Option.contains(Headers.get(response.headers, "x-durable-pusher-shard-full"), "1")) {
            return response;
          }
        }

        const latest = yield* catalog.shardCount(encodedPlacement).pipe(actorApiError);
        const previousCount = shardCount;
        shardCount =
          latest > shardCount
            ? latest
            : yield* catalog.expand(encodedPlacement, shardCount).pipe(actorApiError);
        candidateFloor = Math.min(previousCount, shardCount - 1);
        shardCounts.set(application.appId, shardCount);
      }

      return HttpServerResponse.text("Connection shards are temporarily full", { status: 503 });
    });

    const route = Effect.fn("PusherWorker.route")(function* (
      request: HttpServerRequest.HttpServerRequest,
      pathname: string,
    ) {
      if (request.method === "OPTIONS") {
        return HttpServerResponse.empty({
          headers: {
            "access-control-allow-headers": "Authorization, Content-Type, X-Pusher-Library",
            "access-control-allow-methods": "DELETE, GET, OPTIONS, PATCH, POST",
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
      if (pathname === "/control/v1/apps" || pathname.startsWith("/control/v1/apps/")) {
        const url = yield* Schema.decodeEffect(Schema.URLFromString)(request.originalUrl);
        return yield* applicationsHttp.handle(request, url);
      }

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
        const source = request.source;
        if (!(source instanceof Request)) {
          return HttpServerResponse.text("WebSocket request source is unavailable", {
            status: 500,
          });
        }
        if (Option.isSome(application)) {
          return yield* routeConnection(request, source, application.value);
        }
        const shardName = "invalid:connection:0";
        const routedRequest = yield* Effect.sync(() =>
          HttpServerRequest.fromWeb(
            new Request(source, {
              headers: Headers.set(request.headers, "x-durable-pusher-connection-shard", shardName),
            }),
          ),
        );
        return yield* connections.getByName(shardName).fetch(routedRequest).pipe(actorApiError);
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
