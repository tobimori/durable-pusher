import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import {
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShard,
  FanoutShard,
  UserShard,
} from "./contracts.ts";

type ChannelNamespace = Effect.Success<typeof ChannelShard>;
type ConnectionNamespace = Effect.Success<typeof ConnectionShard>;
type DirectoryNamespace = Effect.Success<typeof ChannelDirectoryShard>;
type FanoutNamespace = Effect.Success<typeof FanoutShard>;
type UserNamespace = Effect.Success<typeof UserShard>;

export class ChannelActorDependencies extends Context.Service<
  ChannelActorDependencies,
  {
    readonly directories: DirectoryNamespace;
    readonly fanouts: FanoutNamespace;
  }
>()("durable-pusher/actors/ChannelActorDependencies") {}

export class ConnectionActorDependencies extends Context.Service<
  ConnectionActorDependencies,
  {
    readonly channels: ChannelNamespace;
    readonly fanouts: FanoutNamespace;
    readonly users: UserNamespace;
  }
>()("durable-pusher/actors/ConnectionActorDependencies") {}

export class FanoutActorDependencies extends Context.Service<
  FanoutActorDependencies,
  {
    readonly channels: ChannelNamespace;
    readonly connections: ConnectionNamespace;
  }
>()("durable-pusher/actors/FanoutActorDependencies") {}

export class UserActorDependencies extends Context.Service<
  UserActorDependencies,
  { readonly connections: ConnectionNamespace }
>()("durable-pusher/actors/UserActorDependencies") {}

export class HttpActorDependencies extends Context.Service<
  HttpActorDependencies,
  {
    readonly channels: ChannelNamespace;
    readonly directories: DirectoryNamespace;
    readonly users: UserNamespace;
  }
>()("durable-pusher/actors/HttpActorDependencies") {}
