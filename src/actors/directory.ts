import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { and, eq, gte, lt } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import migrations from "../db/migrations.ts";
import { directoryChannels } from "../db/schema.ts";
import { mapActorError } from "../pusher/protocol.ts";
import { ChannelDirectoryShard } from "./contracts.ts";

export { ChannelDirectoryShard } from "./contracts.ts";

export const ChannelDirectoryShardLive = ChannelDirectoryShard.make(
  Effect.succeed(
    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations });

      return {
        set: Effect.fn("ChannelDirectoryShard.set")(function* (entry) {
          if (entry.subscriptionCount === 0) {
            yield* db
              .delete(directoryChannels)
              .where(eq(directoryChannels.channel, entry.channel))
              .pipe(Effect.mapError(mapActorError("ChannelDirectoryShard", "set")));
            return;
          }

          const updatedAt = yield* Clock.currentTimeMillis;
          yield* db
            .insert(directoryChannels)
            .values({ ...entry, updatedAt })
            .onConflictDoUpdate({
              target: directoryChannels.channel,
              set: {
                subscriptionCount: entry.subscriptionCount,
                updatedAt,
                userCount: entry.userCount,
              },
            })
            .pipe(Effect.mapError(mapActorError("ChannelDirectoryShard", "set")));
        }),
        list: Effect.fn("ChannelDirectoryShard.list")(function* (prefix) {
          const query = db
            .select({
              channel: directoryChannels.channel,
              subscriptionCount: directoryChannels.subscriptionCount,
              userCount: directoryChannels.userCount,
            })
            .from(directoryChannels);

          return yield* (
            prefix === null
              ? query.orderBy(directoryChannels.channel)
              : query
                  .where(
                    and(
                      gte(directoryChannels.channel, prefix),
                      lt(directoryChannels.channel, `${prefix}\uffff`),
                    ),
                  )
                  .orderBy(directoryChannels.channel)
          ).pipe(Effect.mapError(mapActorError("ChannelDirectoryShard", "list")));
        }),
      };
    }),
  ),
);
