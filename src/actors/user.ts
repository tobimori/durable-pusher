import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import { mapActorError } from "../pusher/protocol.ts";
import { connectionShardCatalogName, connectionShardName, fanoutRelayName } from "../sharding.ts";
import { UserShard, type UserShardApi } from "./contracts.ts";
import { UserActorDependencies } from "./dependencies.ts";

export { UserShard } from "./contracts.ts";

export const UserShardLive = UserShard.make(
  Effect.gen(function* () {
    const { catalogs, relays } = yield* UserActorDependencies;

    const terminate = Effect.fn("UserShard.terminate")(
      function* (placement: ApplicationPlacementEncoded, userId: string) {
        const decodedPlacement = yield* Schema.decodeEffect(ApplicationPlacement)(placement);
        const catalog = yield* catalogs.getByName(
          connectionShardCatalogName(decodedPlacement.appId),
          decodedPlacement,
        );
        const shardCount = yield* catalog.shardCount(placement);
        const path = "terminate.root";
        const relay = yield* relays.getByName(
          fanoutRelayName(decodedPlacement.appId, `#server-to-user-${userId}`, path),
          decodedPlacement,
        );
        return yield* relay.terminateUser(
          placement,
          userId,
          Array.from({ length: shardCount }, (_, shard) =>
            connectionShardName(decodedPlacement.appId, shard),
          ),
          path,
        );
      },
      Effect.mapError(mapActorError("UserShard", "terminate")),
    );

    const api = { terminate } satisfies UserShardApi;
    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.succeed(api);
  }),
);
