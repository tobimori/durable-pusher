import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import {
  AppRegistry,
  ChannelDirectoryShard,
  ChannelShard,
  ConnectionShardCatalog,
  ConnectionShard,
  FanoutRelay,
  UserShard,
} from "./contracts.ts";
import type { PlacedNamespace } from "./placement.ts";

type AppRegistryNamespace = Effect.Success<typeof AppRegistry>;
type ChannelNamespace = Effect.Success<typeof ChannelShard>;
type CatalogNamespace = Effect.Success<typeof ConnectionShardCatalog>;
type ConnectionNamespace = Effect.Success<typeof ConnectionShard>;
type DirectoryNamespace = Effect.Success<typeof ChannelDirectoryShard>;
type RelayNamespace = Effect.Success<typeof FanoutRelay>;
type UserNamespace = Effect.Success<typeof UserShard>;
type ChannelStub = ReturnType<ChannelNamespace["getByName"]>;
type CatalogStub = ReturnType<CatalogNamespace["getByName"]>;
type ConnectionStub = ReturnType<ConnectionNamespace["getByName"]>;
type DirectoryStub = ReturnType<DirectoryNamespace["getByName"]>;
type RelayStub = ReturnType<RelayNamespace["getByName"]>;
type UserStub = ReturnType<UserNamespace["getByName"]>;

export class ChannelActorDependencies extends Context.Service<
  ChannelActorDependencies,
  {
    readonly connections: PlacedNamespace<ConnectionStub>;
    readonly directories: PlacedNamespace<DirectoryStub>;
    readonly relays: PlacedNamespace<RelayStub>;
  }
>()("durable-pusher/actors/ChannelActorDependencies") {}

export class ConnectionActorDependencies extends Context.Service<
  ConnectionActorDependencies,
  {
    readonly applications: AppRegistryNamespace;
    readonly channels: PlacedNamespace<ChannelStub>;
    readonly connectionShardSoftLimit: number;
    readonly users: PlacedNamespace<UserStub>;
  }
>()("durable-pusher/actors/ConnectionActorDependencies") {}

export class FanoutRelayDependencies extends Context.Service<
  FanoutRelayDependencies,
  {
    readonly connections: PlacedNamespace<ConnectionStub>;
    readonly relays: PlacedNamespace<RelayStub>;
  }
>()("durable-pusher/actors/FanoutRelayDependencies") {}

export class UserActorDependencies extends Context.Service<
  UserActorDependencies,
  {
    readonly catalogs: PlacedNamespace<CatalogStub>;
    readonly relays: PlacedNamespace<RelayStub>;
  }
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
