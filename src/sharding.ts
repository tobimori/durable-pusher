import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";

export const CONNECTION_SHARD_COUNT = 256;
export const FANOUT_SHARD_COUNT = 64;
export const DIRECTORY_SHARD_COUNT = 64;

export const stableHash = (value: string): number => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
};

export const randomConnectionShardName = Effect.fn("Sharding.randomConnectionShardName")(
  (appId: string) =>
    Random.nextIntBetween(0, CONNECTION_SHARD_COUNT).pipe(
      Effect.map((shard) => `${appId}:connection:${shard}`),
    ),
);

export const channelShardName = (appId: string, channel: string): string =>
  `${appId}\0channel\0${channel}`;

export const fanoutShardName = (
  appId: string,
  channel: string,
  connectionShardName: string,
): string =>
  `${appId}\0fanout\0${channel}\0${stableHash(connectionShardName) % FANOUT_SHARD_COUNT}`;

export const directoryShardName = (appId: string, channel: string): string =>
  `${appId}:directory:${stableHash(channel) % DIRECTORY_SHARD_COUNT}`;

export const userShardName = (appId: string, userId: string): string =>
  `${appId}\0user\0${userId}`;

export const makeSocketId = Effect.fn("Sharding.makeSocketId")(function* () {
  const now = yield* Clock.currentTimeMillis;
  const left = yield* Random.nextIntBetween(0, 2_147_483_647);
  const right = yield* Random.nextIntBetween(0, 2_147_483_647);
  return `${now}${left}.${right}`;
});
