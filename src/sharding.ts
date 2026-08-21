import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";

export const DIRECTORY_SHARD_COUNT = 64;
export const RELAY_FANOUT_WIDTH = 8;

export const stableHash = (value: string): number => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
};

export const connectionShardName = (appId: string, shard: number): string =>
  `${appId}:connection:${shard}`;

export const connectionShardCatalogName = (appId: string): string => `${appId}\0connection-catalog`;

export const channelShardName = (appId: string, channel: string): string =>
  `${appId}\0channel\0${channel}`;

export const fanoutRelayName = (appId: string, channel: string, path: string): string =>
  `${appId}\0relay\0${channel}\0${path}`;

export const directoryShardName = (appId: string, channel: string): string =>
  `${appId}:directory:${stableHash(channel) % DIRECTORY_SHARD_COUNT}`;

export const userShardName = (appId: string, userId: string): string => `${appId}\0user\0${userId}`;

export const makeSocketId = Effect.fn("Sharding.makeSocketId")(function* () {
  const now = yield* Clock.currentTimeMillis;
  const left = yield* Random.nextIntBetween(0, 2_147_483_647);
  const right = yield* Random.nextIntBetween(0, 2_147_483_647);
  return `${now}${left}.${right}`;
});
