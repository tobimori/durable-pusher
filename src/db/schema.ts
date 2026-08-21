import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const cacheEvents = sqliteTable("channel_cache_event", {
  singleton: integer("singleton").primaryKey(),
  sequence: integer("sequence").notNull(),
  event: text("event").notNull(),
  data: text("data").notNull(),
  expiresAt: integer("expires_at").notNull(),
});

export const channelGateways = sqliteTable("channel_gateways", {
  gatewayName: text("gateway_name").primaryKey(),
  registrationToken: text("registration_token").notNull(),
});

export const connectionShardCatalog = sqliteTable("connection_shard_catalog", {
  singleton: integer("singleton").primaryKey(),
  appId: text("app_id").notNull(),
  jurisdiction: text("jurisdiction", { enum: ["eu", "fedramp", "fedramp-high", "us"] }),
  locationHint: text("location_hint", {
    enum: ["afr", "apac", "apac-ne", "apac-se", "eeur", "enam", "me", "oc", "sam", "weur", "wnam"],
  }),
  shardCount: integer("shard_count").notNull(),
});

export const directoryChannels = sqliteTable("directory_channels", {
  channel: text("channel").primaryKey(),
  subscriptionCount: integer("subscription_count").notNull(),
  userCount: integer("user_count").notNull(),
  updatedAt: integer("updated_at").notNull(),
});
