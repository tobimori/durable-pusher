import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import ChannelHostLive, { ChannelHost } from "./src/hosts/channel.ts";
import ConnectionHostLive, { ConnectionHost } from "./src/hosts/connection.ts";
import DirectoryHostLive, { DirectoryHost } from "./src/hosts/directory.ts";
import FanoutHostLive, { FanoutHost } from "./src/hosts/fanout.ts";
import UserHostLive, { UserHost } from "./src/hosts/user.ts";
import PusherWorkerLive, { PusherWorker } from "./src/worker.ts";

const WorkersLive = Layer.mergeAll(
  PusherWorkerLive,
  ConnectionHostLive,
  ChannelHostLive,
  FanoutHostLive,
  DirectoryHostLive,
  UserHostLive,
);

export default Alchemy.Stack(
  "DurablePusher",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const worker = yield* PusherWorker;
    yield* ConnectionHost;
    yield* ChannelHost;
    yield* FanoutHost;
    yield* DirectoryHost;
    yield* UserHost;
    return {
      name: worker.workerName,
      url: worker.url.as<string>(),
    };
  }).pipe(Effect.provide(WorkersLive)),
);
