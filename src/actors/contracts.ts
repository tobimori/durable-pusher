import type { RuntimeContext } from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import type * as Effect from "effect/Effect";
import type {
  ApplicationBootstrapEncoded,
  ApplicationCreateEncoded,
  ApplicationPatchEncoded,
  ApplicationPlacementEncoded,
  ApplicationSummaryEncoded,
  ProvisionedApplicationEncoded,
  RuntimeApplicationEncoded,
} from "../apps/model.ts";
import type {
  ActorError,
  ChannelInfo,
  ChannelSnapshot,
  DeliveryEncoded,
  DirectoryEntry,
  PresenceConnection,
  PresenceJoinEncoded,
  PresenceSnapshot,
} from "../pusher/protocol.ts";

// RPC methods must be functions even when their only argument is the call itself.
// @effect-diagnostics lazyEffect:off

type ActorEffect<A> = Effect.Effect<A, ActorError, RuntimeContext>;

export interface ConnectionShardApi {
  readonly count: (channel: string) => ActorEffect<number>;
  readonly deliver: (delivery: DeliveryEncoded) => ActorEffect<number>;
  readonly presence: (channel: string) => ActorEffect<ReadonlyArray<PresenceConnection>>;
  readonly terminateApplication: (appId: string) => ActorEffect<number>;
  readonly terminateUser: (appId: string, userId: string) => ActorEffect<number>;
}

export interface ChannelShardApi {
  readonly broadcastSubscriptionCount: (
    placement: ApplicationPlacementEncoded,
    channel: string,
  ) => ActorEffect<void>;
  readonly registerGateway: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    gatewayName: string,
    registrationToken: string,
  ) => ActorEffect<ChannelSnapshot>;
  readonly snapshot: (
    placement: ApplicationPlacementEncoded,
    channel: string,
  ) => ActorEffect<ChannelSnapshot>;
  readonly joinPresence: (join: PresenceJoinEncoded) => ActorEffect<PresenceSnapshot>;
  readonly leavePresence: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    socketId: string,
    userId: string,
    active: boolean,
  ) => ActorEffect<void>;
  readonly unregisterGateway: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    gatewayName: string,
    registrationToken: string,
  ) => ActorEffect<void>;
  readonly publish: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    event: string,
    data: string,
    excludedSocketId: string | null,
    userId: string | null,
    updateCache: boolean,
  ) => ActorEffect<void>;
  readonly info: (
    placement: ApplicationPlacementEncoded,
    channel: string,
  ) => ActorEffect<ChannelInfo>;
  readonly presenceUsers: (
    placement: ApplicationPlacementEncoded,
    channel: string,
  ) => ActorEffect<ReadonlyArray<string>>;
}

export interface ConnectionShardCatalogApi {
  readonly expand: (
    placement: ApplicationPlacementEncoded,
    expectedShardCount: number,
  ) => ActorEffect<number>;
  readonly shardCount: (placement: ApplicationPlacementEncoded) => ActorEffect<number>;
}

export interface FanoutRelayApi {
  readonly count: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    gatewayNames: ReadonlyArray<string>,
    path: string,
  ) => ActorEffect<number>;
  readonly deliver: (
    delivery: DeliveryEncoded,
    gatewayNames: ReadonlyArray<string>,
    path: string,
  ) => ActorEffect<ReadonlyArray<string>>;
  readonly presence: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    gatewayNames: ReadonlyArray<string>,
    path: string,
  ) => ActorEffect<ReadonlyArray<PresenceConnection>>;
  readonly terminateApplication: (
    placement: ApplicationPlacementEncoded,
    gatewayNames: ReadonlyArray<string>,
    path: string,
  ) => ActorEffect<number>;
  readonly terminateUser: (
    placement: ApplicationPlacementEncoded,
    userId: string,
    gatewayNames: ReadonlyArray<string>,
    path: string,
  ) => ActorEffect<number>;
}

export interface ChannelDirectoryShardApi {
  readonly set: (entry: DirectoryEntry) => ActorEffect<void>;
  readonly list: (prefix: string | null) => ActorEffect<ReadonlyArray<DirectoryEntry>>;
}

export interface AppRegistryApi {
  readonly bootstrap: (
    input: ApplicationBootstrapEncoded,
  ) => ActorEffect<RuntimeApplicationEncoded>;
  readonly create: (input: ApplicationCreateEncoded) => ActorEffect<ProvisionedApplicationEncoded>;
  readonly get: (appId: string) => ActorEffect<ApplicationSummaryEncoded | null>;
  readonly list: () => ActorEffect<ReadonlyArray<ApplicationSummaryEncoded>>;
  readonly remove: (appId: string) => ActorEffect<boolean>;
  readonly resolveByAuthToken: (authToken: string) => ActorEffect<RuntimeApplicationEncoded | null>;
  readonly resolveById: (appId: string) => ActorEffect<RuntimeApplicationEncoded | null>;
  readonly resolveByKey: (appKey: string) => ActorEffect<RuntimeApplicationEncoded | null>;
  readonly update: (
    appId: string,
    patch: ApplicationPatchEncoded,
  ) => ActorEffect<ApplicationSummaryEncoded>;
}

export class ConnectionShard extends Cloudflare.DurableObject<
  ConnectionShard,
  ConnectionShardApi
>()("ConnectionShard") {}

export class ChannelShard extends Cloudflare.DurableObject<ChannelShard, ChannelShardApi>()(
  "ChannelShard",
) {}

export class ConnectionShardCatalog extends Cloudflare.DurableObject<
  ConnectionShardCatalog,
  ConnectionShardCatalogApi
>()("ConnectionShardCatalog") {}

export class FanoutRelay extends Cloudflare.DurableObject<FanoutRelay, FanoutRelayApi>()(
  "FanoutRelay",
) {}

export class ChannelDirectoryShard extends Cloudflare.DurableObject<
  ChannelDirectoryShard,
  ChannelDirectoryShardApi
>()("ChannelDirectoryShard") {}

export class AppRegistry extends Cloudflare.DurableObject<AppRegistry, AppRegistryApi>()(
  "AppRegistry",
) {}

export class PusherWorker extends Cloudflare.Worker<
  PusherWorker,
  {},
  | AppRegistry
  | ChannelDirectoryShard
  | ChannelShard
  | ConnectionShardCatalog
  | ConnectionShard
  | FanoutRelay
>()("DurablePusher") {}
