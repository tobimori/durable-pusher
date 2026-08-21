import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import {
  AppRegistry,
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShard,
  FanoutShard,
  UserShard,
} from "./contracts.ts";
import type { PlacedNamespace } from "./placement.ts";

type AppRegistryNamespace = Effect.Success<typeof AppRegistry>;
type ChannelNamespace = Effect.Success<typeof ChannelShard>;
type ConnectionNamespace = Effect.Success<typeof ConnectionShard>;
type DirectoryNamespace = Effect.Success<typeof ChannelDirectoryShard>;
type FanoutNamespace = Effect.Success<typeof FanoutShard>;
type UserNamespace = Effect.Success<typeof UserShard>;
type ChannelStub = ReturnType<ChannelNamespace["getByName"]>;
type ConnectionStub = ReturnType<ConnectionNamespace["getByName"]>;
type DirectoryStub = ReturnType<DirectoryNamespace["getByName"]>;
type FanoutStub = ReturnType<FanoutNamespace["getByName"]>;
type UserStub = ReturnType<UserNamespace["getByName"]>;

export class ChannelActorDependencies extends Context.Service<
  ChannelActorDependencies,
  {
    readonly directories: PlacedNamespace<DirectoryStub>;
    readonly fanouts: PlacedNamespace<FanoutStub>;
  }
>()("durable-pusher/actors/ChannelActorDependencies") {}

export class ConnectionActorDependencies extends Context.Service<
  ConnectionActorDependencies,
  {
    readonly applications: AppRegistryNamespace;
    readonly channels: PlacedNamespace<ChannelStub>;
    readonly fanouts: PlacedNamespace<FanoutStub>;
    readonly users: PlacedNamespace<UserStub>;
  }
>()("durable-pusher/actors/ConnectionActorDependencies") {}

export class FanoutActorDependencies extends Context.Service<
  FanoutActorDependencies,
  {
    readonly channels: PlacedNamespace<ChannelStub>;
    readonly connections: PlacedNamespace<ConnectionStub>;
  }
>()("durable-pusher/actors/FanoutActorDependencies") {}

export class UserActorDependencies extends Context.Service<
  UserActorDependencies,
  { readonly connections: PlacedNamespace<ConnectionStub> }
>()("durable-pusher/actors/UserActorDependencies") {}

export class HttpActorDependencies extends Context.Service<
  HttpActorDependencies,
  {
    readonly applications: AppRegistryNamespace;
    readonly channels: PlacedNamespace<ChannelStub>;
    readonly directories: PlacedNamespace<DirectoryStub>;
    readonly users: PlacedNamespace<UserStub>;
  }
>()("durable-pusher/actors/HttpActorDependencies") {}
