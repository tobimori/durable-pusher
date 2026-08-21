import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChannelDirectoryShard, DirectoryHost } from "../actors/contracts.ts";
import { ChannelDirectoryShardLive } from "../actors/directory.ts";
import { WorkerNames, WorkerNamesLive } from "./names.ts";

export { DirectoryHost };

export const DirectoryHostLive = DirectoryHost.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return { main: import.meta.url, name: names.directory };
  }),
  Effect.gen(function* () {
    yield* ChannelDirectoryShard;
    return {};
  }).pipe(Effect.provide(ChannelDirectoryShardLive)),
).pipe(Layer.provide(WorkerNamesLive));

export default DirectoryHostLive;
