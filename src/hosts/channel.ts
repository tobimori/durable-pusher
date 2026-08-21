import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChannelShardLive } from "../actors/channel.ts";
import {
  ChannelDirectoryShard,
  ChannelHost,
  ChannelShard,
  FanoutShard,
} from "../actors/contracts.ts";
import { ChannelActorDependencies } from "../actors/dependencies.ts";
import { WorkerNames, WorkerNamesLive } from "./names.ts";

export { ChannelHost };

export const ChannelHostLive = ChannelHost.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return { main: import.meta.url, name: names.channel };
  }),
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    const fanouts = yield* FanoutShard.from(names.fanout);
    const directories = yield* ChannelDirectoryShard.from(names.directory);
    const dependencies = Layer.succeed(ChannelActorDependencies, { directories, fanouts });
    yield* ChannelShard.pipe(
      Effect.provide(ChannelShardLive.pipe(Layer.provide(dependencies))),
    );
    return {};
  }),
).pipe(Layer.provide(WorkerNamesLive));

export default ChannelHostLive;
