import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  ChannelShard,
  ConnectionShard,
  FanoutHost,
  FanoutShard,
} from "../actors/contracts.ts";
import { FanoutActorDependencies } from "../actors/dependencies.ts";
import { FanoutShardLive } from "../actors/fanout.ts";
import { WorkerNames, WorkerNamesLive } from "./names.ts";

export { FanoutHost };

export const FanoutHostLive = FanoutHost.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return { main: import.meta.url, name: names.fanout };
  }),
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    const channels = yield* ChannelShard.from(names.channel);
    const connections = yield* ConnectionShard.from(names.connection);
    const dependencies = Layer.succeed(FanoutActorDependencies, { channels, connections });
    yield* FanoutShard.pipe(
      Effect.provide(FanoutShardLive.pipe(Layer.provide(dependencies))),
    );
    return {};
  }),
).pipe(Layer.provide(WorkerNamesLive));

export default FanoutHostLive;
