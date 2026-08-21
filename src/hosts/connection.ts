import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ConnectionShardLive } from "../actors/connection.ts";
import {
  AppRegistry,
  ChannelShard,
  ConnectionHost,
  ConnectionShard,
  FanoutShard,
  UserShard,
} from "../actors/contracts.ts";
import { ConnectionActorDependencies } from "../actors/dependencies.ts";
import { makePlacedNamespace } from "../actors/placement.ts";
import { WorkerNames, WorkerNamesLive } from "./names.ts";

export { ConnectionHost };

export const ConnectionHostLive = ConnectionHost.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return { main: import.meta.url, name: names.connection };
  }),
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    const environment = yield* Cloudflare.WorkerEnvironment;
    const applications = yield* AppRegistry.from(names.registry);
    const fanouts = yield* FanoutShard.from(names.fanout);
    const channels = yield* ChannelShard.from(names.channel);
    const users = yield* UserShard.from(names.user);
    const dependencies = Layer.succeed(ConnectionActorDependencies, {
      applications,
      channels: makePlacedNamespace("ChannelShard", channels, environment),
      fanouts: makePlacedNamespace("FanoutShard", fanouts, environment),
      users: makePlacedNamespace("UserShard", users, environment),
    });
    yield* ConnectionShard.pipe(
      Effect.provide(ConnectionShardLive.pipe(Layer.provide(dependencies))),
    );
    return {};
  }),
).pipe(Layer.provide(WorkerNamesLive));

export default ConnectionHostLive;
