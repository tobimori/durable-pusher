import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ConnectionShard, UserHost, UserShard } from "../actors/contracts.ts";
import { UserActorDependencies } from "../actors/dependencies.ts";
import { makePlacedNamespace } from "../actors/placement.ts";
import { UserShardLive } from "../actors/user.ts";
import { WorkerNames, WorkerNamesLive } from "./names.ts";

export { UserHost };

export const UserHostLive = UserHost.make(
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    return { main: import.meta.url, name: names.user };
  }),
  Effect.gen(function* () {
    const names = yield* WorkerNames;
    const environment = yield* Cloudflare.WorkerEnvironment;
    const connections = yield* ConnectionShard.from(names.connection);
    const dependencies = Layer.succeed(UserActorDependencies, {
      connections: makePlacedNamespace("ConnectionShard", connections, environment),
    });
    yield* UserShard.pipe(Effect.provide(UserShardLive.pipe(Layer.provide(dependencies))));
    return {};
  }),
).pipe(Layer.provide(WorkerNamesLive));

export default UserHostLive;
