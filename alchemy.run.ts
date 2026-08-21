import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import PusherWorkerLive, { PusherWorker } from "./src/worker.ts";

export default Alchemy.Stack(
  "DurablePusher",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const worker = yield* PusherWorker;
    return {
      name: worker.workerName,
      url: worker.url.as<string>(),
    };
  }).pipe(Effect.provide(PusherWorkerLive)),
);
