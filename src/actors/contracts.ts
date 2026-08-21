import type { RuntimeContext } from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import type * as Effect from "effect/Effect";
import type {
  ActorError,
  ChannelInfo,
  ChannelSnapshot,
  Delivery,
  DirectoryEntry,
  PresenceJoin,
  PresenceSnapshot,
} from "../pusher/protocol.ts";

// RPC methods must be functions even when their only argument is the call itself.
// @effect-diagnostics lazyEffect:off

type ActorEffect<A> = Effect.Effect<A, ActorError, RuntimeContext>;

export interface ConnectionShardApi {
  readonly deliver: (delivery: Delivery) => ActorEffect<number>;
  readonly terminateUser: (userId: string) => ActorEffect<number>;
}

export interface ChannelShardApi {
  readonly setBranch: (
    appId: string,
    channel: string,
    branchName: string,
    subscriptionCount: number,
    generation: number,
  ) => ActorEffect<ChannelSnapshot>;
  readonly joinPresence: (join: PresenceJoin) => ActorEffect<PresenceSnapshot>;
  readonly leavePresence: (appId: string, channel: string, socketId: string) => ActorEffect<void>;
  readonly publish: (
    appId: string,
    channel: string,
    event: string,
    data: string,
    excludedSocketId: string | null,
    userId: string | null,
    updateCache: boolean,
  ) => ActorEffect<ChannelInfo>;
  readonly info: () => ActorEffect<ChannelInfo>;
  readonly presenceUsers: () => ActorEffect<ReadonlyArray<string>>;
}

export interface FanoutShardApi {
  readonly setGateway: (
    appId: string,
    channel: string,
    branchName: string,
    gatewayName: string,
    subscriptionCount: number,
    gatewayGeneration: number,
  ) => ActorEffect<ChannelSnapshot>;
  readonly deliver: (delivery: Delivery) => ActorEffect<void>;
}

export interface ChannelDirectoryShardApi {
  readonly set: (entry: DirectoryEntry) => ActorEffect<void>;
  readonly list: (prefix: string | null) => ActorEffect<ReadonlyArray<DirectoryEntry>>;
}

export interface UserShardApi {
  readonly setGateway: (
    gatewayName: string,
    connectionCount: number,
    generation: number,
  ) => ActorEffect<void>;
  readonly terminate: (userId: string) => ActorEffect<number>;
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

export class ConnectionHost extends Cloudflare.Worker<ConnectionHost, {}, ConnectionShard>()(
  "ConnectionHost",
) {}

export class ChannelHost extends Cloudflare.Worker<ChannelHost, {}, ChannelShard>()("ChannelHost") {}

export class FanoutHost extends Cloudflare.Worker<FanoutHost, {}, FanoutShard>()("FanoutHost") {}

export class DirectoryHost extends Cloudflare.Worker<
  DirectoryHost,
  {},
  ChannelDirectoryShard
>()("DirectoryHost") {}

export class UserHost extends Cloudflare.Worker<UserHost, {}, UserShard>()("UserHost") {}

export class PusherWorker extends Cloudflare.Worker<PusherWorker, {}>()("DurablePusher") {}
