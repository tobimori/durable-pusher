import * as Cloudflare from "alchemy/Cloudflare";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Random from "effect/Random";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { ChannelShardLive } from "./actors/channel.ts";
import { ApplicationAuthorityLive } from "./actors/application.ts";
import { ConnectionShardCatalogLive } from "./actors/catalog.ts";
import { ConnectionShardLive } from "./actors/connection.ts";
import {
  AppRegistry,
  ApplicationAuthority,
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShardCatalog,
  ConnectionShard,
  FanoutRelay,
  PusherWorker,
} from "./actors/contracts.ts";
import {
  ChannelActorDependencies,
  AppRegistryDependencies,
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
  ApplicationAuthorityState,
  AppKey,
  ApplicationPlacement,
  ResolvedApplication,
  type ApplicationPlacementEncoded,
  type ResolvedApplication as ResolvedApplicationType,
} from "./apps/model.ts";
import { makeApplicationsHttp } from "./apps/http.ts";
import { AppConfig, AppConfigLive } from "./config.ts";
import { WorkerNames, WorkerNamesLive } from "./hosts/names.ts";
import { makePusherHttp } from "./pusher/http.ts";
import { sha256Hex } from "./pusher/crypto.ts";
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
const APPLICATION_CACHE_TTL_MS = 1_000;
const APPLICATION_NEGATIVE_CACHE_TTL_MS = 250;
const CACHE_ENTRY_LIMIT = 1_024;
const CONNECTION_ROUTE_CACHE_TTL_MS = 5_000;

interface CacheLock {
  readonly semaphore: Semaphore.Semaphore;
  users: number;
}

interface ApplicationCacheEntry {
  readonly application: ResolvedApplicationType | null;
  readonly expiresAt: number;
}

interface ConnectionRouteCacheEntry {
  readonly expiresAt: number;
  readonly generation: number;
  readonly shardCount: number;
}

const setCacheEntry = <A>(cache: Map<string, A>, key: string, value: A): void => {
  cache.delete(key);
  if (cache.size >= CACHE_ENTRY_LIMIT) {
    const oldest = cache.keys().next();
    if (!oldest.done) {
      cache.delete(oldest.value);
    }
  }
  cache.set(key, value);
};

const withCacheLock =
  (locks: Map<string, CacheLock>, key: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
    let entry = locks.get(key);
    if (entry === undefined) {
      entry = { semaphore: Semaphore.makeUnsafe(1), users: 0 };
      locks.set(key, entry);
    }
    entry.users += 1;
    return effect.pipe(
      Semaphore.withPermit(entry.semaphore),
      Effect.ensuring(
        Effect.sync(() => {
          entry.users -= 1;
          if (entry.users === 0 && locks.get(key) === entry) {
            locks.delete(key);
          }
        }),
      ),
    );
  };

const apiError = (status: number, message: string): ApiError => ApiError.make({ message, status });

const actorApiError = Effect.mapError((error: { readonly message: string }) =>
  apiError(503, error.message),
);

const placementOf = (application: ResolvedApplicationType): ApplicationPlacement => ({
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
    const authorities = yield* ApplicationAuthority.from(PusherWorker);
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
      ApplicationAuthorityLive,
      AppRegistryLive.pipe(Layer.provide(Layer.succeed(AppRegistryDependencies, { authorities }))),
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
            authorities,
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
        ApplicationAuthority,
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
      authorities,
      catalogs: placedCatalogs,
      channels: placedChannels,
      directories: placedDirectories,
      relays: placedRelays,
    });
    const http = yield* makePusherHttp.pipe(Effect.provide(httpDependencies));
    const applicationCache = new Map<string, ApplicationCacheEntry>();
    const applicationCacheLocks = new Map<string, CacheLock>();
    const connectionRouteCache = new Map<string, ConnectionRouteCacheEntry>();
    const connectionRouteCacheLocks = new Map<string, CacheLock>();
    const terminateApplication = Effect.fn("PusherWorker.terminateApplication")(function* (
      encodedPlacement: ApplicationPlacementEncoded,
      generation: number,
    ) {
      const placement = yield* Schema.decodeEffect(ApplicationPlacement)(encodedPlacement);
      connectionRouteCache.delete(placement.appId);
      for (const [appKey, cached] of applicationCache) {
        if (cached.application?.appId === placement.appId) {
          applicationCache.delete(appKey);
        }
      }
      const catalog = yield* placedCatalogs.getByName(
        connectionShardCatalogName(placement.appId),
        placement,
      );
      const shardCount = yield* catalog.fence(encodedPlacement, generation);
      const path = "terminate.root";
      const relay = yield* placedRelays.getByName(
        fanoutRelayName(placement.appId, "application-control", path),
        placement,
      );
      return yield* relay.terminateApplication(
        encodedPlacement,
        generation,
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
    const bootstrapCreatedAt = yield* Clock.currentTimeMillis;
    const bootstrapAuthorityInput = yield* Schema.encodeEffect(ApplicationAuthorityState)({
      appId: config.appId,
      appKey: config.appKey,
      appSecret: Redacted.value(config.appSecret),
      authTokenHash: sha256Hex(Redacted.value(config.authToken)),
      createdAt: bootstrapCreatedAt,
      encryptionMasterKey: Redacted.value(config.encryptionMasterKey),
      generation: 0,
      jurisdiction: Option.none(),
      locationHint: Option.none(),
      name: "Bootstrap application",
      status: "active",
      updatedAt: bootstrapCreatedAt,
    }).pipe(Effect.orDie);
    const authorityBootstrapLock = yield* Semaphore.make(1);
    let authorityBootstrapComplete = false;
    const bootstrapAuthority = Effect.fn("PusherWorker.bootstrapAuthority")(
      function* () {
        if (authorityBootstrapComplete) {
          return;
        }
        yield* Effect.gen(function* () {
          if (authorityBootstrapComplete) {
            return;
          }
          yield* authorities.getByName(config.appKey).initialize(bootstrapAuthorityInput);
          authorityBootstrapComplete = true;
        }).pipe(Semaphore.withPermit(authorityBootstrapLock));
      },
      Effect.mapError(() => apiError(503, "Application authority is unavailable")),
    );
    const directoryBootstrapLock = yield* Semaphore.make(1);
    let directoryBootstrapComplete = false;
    const bootstrapDirectory = Effect.fn("PusherWorker.bootstrapDirectory")(
      function* () {
        if (directoryBootstrapComplete) {
          return;
        }
        yield* Effect.gen(function* () {
          if (directoryBootstrapComplete) {
            return;
          }
          yield* applications.getByName("applications").bootstrap(bootstrapInput);
          directoryBootstrapComplete = true;
        }).pipe(Semaphore.withPermit(directoryBootstrapLock));
      },
      Effect.mapError(() => apiError(503, "Application directory is unavailable")),
    );
    const resolveApplicationByKey = Effect.fn("PusherWorker.resolveApplicationByKey")(function* (
      appKey: string,
    ) {
      const decodedKey = yield* Schema.decodeEffect(AppKey)(appKey).pipe(Effect.result);
      if (Result.isFailure(decodedKey)) {
        return Option.none<ResolvedApplicationType>();
      }
      const key = decodedKey.success;
      const now = yield* Clock.currentTimeMillis;
      const cached = applicationCache.get(key);
      if (cached !== undefined && cached.expiresAt > now) {
        return Option.fromNullishOr(cached.application);
      }
      return yield* Effect.gen(function* () {
        const lockedNow = yield* Clock.currentTimeMillis;
        const lockedCached = applicationCache.get(key);
        if (lockedCached !== undefined && lockedCached.expiresAt > lockedNow) {
          return Option.fromNullishOr(lockedCached.application);
        }
        const authority = authorities.getByName(key);
        let encoded = Option.fromNullishOr(yield* authority.resolve().pipe(actorApiError));
        if (Option.isNone(encoded) && key === config.appKey) {
          yield* bootstrapAuthority();
          encoded = Option.fromNullishOr(yield* authority.resolve().pipe(actorApiError));
        }
        const application = Option.isNone(encoded)
          ? null
          : yield* Schema.decodeEffect(ResolvedApplication)(encoded.value).pipe(actorApiError);
        setCacheEntry(applicationCache, key, {
          application,
          expiresAt:
            lockedNow +
            (application === null ? APPLICATION_NEGATIVE_CACHE_TTL_MS : APPLICATION_CACHE_TTL_MS),
        });
        return Option.fromNullishOr(application);
      }).pipe(withCacheLock(applicationCacheLocks, key));
    });

    const routeConnection = Effect.fn("PusherWorker.routeConnection")(function* (
      request: HttpServerRequest.HttpServerRequest,
      source: Request,
      application: ResolvedApplicationType,
    ) {
      const placement = placementOf(application);
      const encodedPlacement =
        yield* Schema.encodeEffect(ApplicationPlacement)(placement).pipe(actorApiError);
      const catalog = yield* placedCatalogs
        .getByName(connectionShardCatalogName(application.appId), placement)
        .pipe(actorApiError);
      const readShardCount = () =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const cached = connectionRouteCache.get(application.appId);
          if (
            cached !== undefined &&
            cached.expiresAt > now &&
            cached.generation === application.generation
          ) {
            return cached.shardCount;
          }
          return yield* Effect.gen(function* () {
            const lockedNow = yield* Clock.currentTimeMillis;
            const lockedCached = connectionRouteCache.get(application.appId);
            if (
              lockedCached !== undefined &&
              lockedCached.expiresAt > lockedNow &&
              lockedCached.generation === application.generation
            ) {
              return lockedCached.shardCount;
            }
            const shardCount = yield* catalog
              .route(encodedPlacement, application.generation)
              .pipe(actorApiError);
            setCacheEntry(connectionRouteCache, application.appId, {
              expiresAt: lockedNow + CONNECTION_ROUTE_CACHE_TTL_MS,
              generation: application.generation,
              shardCount,
            });
            return shardCount;
          }).pipe(withCacheLock(connectionRouteCacheLocks, application.appId));
        });
      const expandShardCount = (expectedShardCount: number) =>
        Effect.gen(function* () {
          const cached = connectionRouteCache.get(application.appId);
          if (
            cached !== undefined &&
            cached.generation === application.generation &&
            cached.shardCount > expectedShardCount
          ) {
            return cached.shardCount;
          }
          return yield* Effect.gen(function* () {
            const lockedCached = connectionRouteCache.get(application.appId);
            if (
              lockedCached !== undefined &&
              lockedCached.generation === application.generation &&
              lockedCached.shardCount > expectedShardCount
            ) {
              return lockedCached.shardCount;
            }
            const latest = yield* catalog
              .route(encodedPlacement, application.generation)
              .pipe(actorApiError);
            const shardCount =
              latest > expectedShardCount
                ? latest
                : yield* catalog
                    .expand(encodedPlacement, application.generation, expectedShardCount)
                    .pipe(actorApiError);
            setCacheEntry(connectionRouteCache, application.appId, {
              expiresAt: (yield* Clock.currentTimeMillis) + CONNECTION_ROUTE_CACHE_TTL_MS,
              generation: application.generation,
              shardCount,
            });
            return shardCount;
          }).pipe(withCacheLock(connectionRouteCacheLocks, application.appId));
        });
      let shardCount = yield* readShardCount();
      let candidateFloor = 0;
      const applicationJson = yield* Schema.encodeEffect(
        Schema.fromJsonString(ResolvedApplication),
      )(application).pipe(actorApiError);
      const applicationHeader = yield* Schema.encodeEffect(Schema.Uint8ArrayFromBase64)(
        new TextEncoder().encode(applicationJson),
      ).pipe(actorApiError);

      for (let round = 0; round < 32; round += 1) {
        const attempts = Math.min(shardCount, 8);
        const tried = new Set<number>();
        for (let attempt = 0; attempt < attempts; attempt += 1) {
          let shard =
            attempt === 0 && candidateFloor < shardCount
              ? yield* Random.nextIntBetween(candidateFloor, shardCount, { halfOpen: true })
              : yield* Random.nextIntBetween(0, shardCount, { halfOpen: true });
          while (tried.has(shard) && tried.size < shardCount) {
            shard = (shard + 1) % shardCount;
          }
          tried.add(shard);
          const shardName = connectionShardName(application.appId, shard);
          const routedRequest = yield* Effect.sync(() =>
            HttpServerRequest.fromWeb(
              new Request(source, {
                headers: Headers.set(
                  Headers.set(request.headers, "x-durable-pusher-application", applicationHeader),
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

        const previousCount = shardCount;
        shardCount = yield* expandShardCount(shardCount);
        candidateFloor = Math.min(previousCount, shardCount - 1);
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
      if (pathname === "/control/v1/apps" || pathname.startsWith("/control/v1/apps/")) {
        yield* bootstrapDirectory();
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
        const application = Result.isSuccess(keyResult)
          ? yield* resolveApplicationByKey(keyResult.success)
          : Option.none<ResolvedApplicationType>();
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
              headers: Headers.set(
                Headers.set(request.headers, "x-durable-pusher-application", ""),
                "x-durable-pusher-connection-shard",
                shardName,
              ),
            }),
          ),
        );
        return yield* connections.getByName(shardName).fetch(routedRequest).pipe(actorApiError);
      }
      yield* bootstrapAuthority();
      if (pathname === "/pusher/auth" || pathname === "/pusher/user-auth") {
        yield* bootstrapDirectory();
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
