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
  PresenceJoinEncoded,
  PresenceSnapshot,
} from "../pusher/protocol.ts";

// RPC methods must be functions even when their only argument is the call itself.
// @effect-diagnostics lazyEffect:off

type ActorEffect<A> = Effect.Effect<A, ActorError, RuntimeContext>;

export interface ConnectionShardApi {
  readonly deliver: (delivery: DeliveryEncoded) => ActorEffect<number>;
  readonly terminateUser: (appId: string, userId: string) => ActorEffect<number>;
}

export interface ChannelShardApi {
  readonly setBranch: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    branchName: string,
    subscriptionCount: number,
    generation: number,
  ) => ActorEffect<ChannelSnapshot>;
  readonly joinPresence: (join: PresenceJoinEncoded) => ActorEffect<PresenceSnapshot>;
  readonly leavePresence: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    socketId: string,
  ) => ActorEffect<void>;
  readonly publish: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    event: string,
    data: string,
    excludedSocketId: string | null,
    userId: string | null,
    updateCache: boolean,
  ) => ActorEffect<ChannelInfo>;
  readonly info: (
    placement: ApplicationPlacementEncoded,
    channel: string,
  ) => ActorEffect<ChannelInfo>;
  readonly presenceUsers: (
    placement: ApplicationPlacementEncoded,
    channel: string,
  ) => ActorEffect<ReadonlyArray<string>>;
}

export interface FanoutShardApi {
  readonly setGateway: (
    placement: ApplicationPlacementEncoded,
    channel: string,
    branchName: string,
    gatewayName: string,
    subscriptionCount: number,
    gatewayGeneration: number,
  ) => ActorEffect<ChannelSnapshot>;
  readonly deliver: (delivery: DeliveryEncoded) => ActorEffect<void>;
}

export interface ChannelDirectoryShardApi {
  readonly set: (entry: DirectoryEntry) => ActorEffect<void>;
  readonly list: (prefix: string | null) => ActorEffect<ReadonlyArray<DirectoryEntry>>;
}

export interface UserShardApi {
  readonly setGateway: (
    placement: ApplicationPlacementEncoded,
    userId: string,
    gatewayName: string,
    connectionCount: number,
    generation: number,
  ) => ActorEffect<void>;
  readonly terminate: (
    placement: ApplicationPlacementEncoded,
    userId: string,
  ) => ActorEffect<number>;
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

export class FanoutShard extends Cloudflare.DurableObject<FanoutShard, FanoutShardApi>()(
  "FanoutShard",
) {}

export class ChannelDirectoryShard extends Cloudflare.DurableObject<
  ChannelDirectoryShard,
  ChannelDirectoryShardApi
>()("ChannelDirectoryShard") {}

export class UserShard extends Cloudflare.DurableObject<UserShard, UserShardApi>()("UserShard") {}

export class AppRegistry extends Cloudflare.DurableObject<AppRegistry, AppRegistryApi>()(
  "AppRegistry",
) {}

export class ConnectionHost extends Cloudflare.Worker<ConnectionHost, {}, ConnectionShard>()(
  "ConnectionHost",
) {}

export class ChannelHost extends Cloudflare.Worker<ChannelHost, {}, ChannelShard>()(
  "ChannelHost",
) {}

export class FanoutHost extends Cloudflare.Worker<FanoutHost, {}, FanoutShard>()("FanoutHost") {}

export class DirectoryHost extends Cloudflare.Worker<DirectoryHost, {}, ChannelDirectoryShard>()(
  "DirectoryHost",
) {}

export class UserHost extends Cloudflare.Worker<UserHost, {}, UserShard>()("UserHost") {}

export class RegistryHost extends Cloudflare.Worker<RegistryHost, {}, AppRegistry>()(
  "RegistryHost",
) {}

export class PusherWorker extends Cloudflare.Worker<PusherWorker, {}>()("DurablePusher") {}
