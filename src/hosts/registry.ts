import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { AppRegistry, RegistryHost } from "../actors/contracts.ts";
import { AppRegistryLive } from "../actors/registry.ts";
import { WorkerNames, WorkerNamesLive } from "./names.ts";

export { RegistryHost };

export const RegistryHostLive = RegistryHost.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return { main: import.meta.url, name: names.registry };
  }),
  Effect.gen(function* () {
    yield* AppRegistry.pipe(Effect.provide(AppRegistryLive));
    return {};
  }),
).pipe(Layer.provide(WorkerNamesLive));

export default RegistryHostLive;
