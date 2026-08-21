import { integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core";

export const connections = sqliteTable("connection_sockets", {
  socketId: text("socket_id").primaryKey(),
  appId: text("app_id").notNull(),
  protocol: integer("protocol").notNull(),
  shardName: text("shard_name").notNull(),
  userId: text("user_id"),
  userData: text("user_data"),
  eventWindow: integer("event_window").notNull(),
  eventCount: integer("event_count").notNull(),
});

export const subscriptions = sqliteTable(
  "connection_subscriptions",
  {
    socketId: text("socket_id").notNull(),
    channel: text("channel").notNull(),
    branchName: text("branch_name").notNull(),
    kind: text("kind", { enum: ["public", "private", "presence", "encrypted"] }).notNull(),
    state: text("state", { enum: ["joining", "active"] }).notNull(),
    startSequence: integer("start_sequence").notNull(),
    lastSequence: integer("last_sequence").notNull(),
    userId: text("user_id"),
    userInfo: text("user_info"),
  },
  (table) => [primaryKey({ columns: [table.socketId, table.channel] })],
);

export const channelVersions = sqliteTable("connection_channel_versions", {
  channel: text("channel").primaryKey(),
  generation: integer("generation").notNull(),
});

export const userVersions = sqliteTable("connection_user_versions", {
  userId: text("user_id").primaryKey(),
  generation: integer("generation").notNull(),
});

export const pendingEvents = sqliteTable(
  "connection_pending_events",
  {
    socketId: text("socket_id").notNull(),
    channel: text("channel").notNull(),
    sequence: integer("sequence").notNull(),
    event: text("event").notNull(),
    data: text("data").notNull(),
    userId: text("user_id"),
  },
  (table) => [primaryKey({ columns: [table.socketId, table.channel, table.sequence] })],
);

export const channelMetadata = sqliteTable("channel_metadata", {
  singleton: integer("singleton").primaryKey(),
  appId: text("app_id").notNull(),
  channel: text("channel").notNull(),
});

export const channelState = sqliteTable("channel_state", {
  singleton: integer("singleton").primaryKey(),
  sequence: integer("sequence").notNull(),
});

export const channelBranches = sqliteTable("channel_branches", {
  branchName: text("branch_name").primaryKey(),
  subscriptionCount: integer("subscription_count").notNull(),
  generation: integer("generation").notNull(),
});

export const presenceMembers = sqliteTable("channel_presence", {
  socketId: text("socket_id").primaryKey(),
  branchName: text("branch_name").notNull(),
  userId: text("user_id").notNull(),
  userInfo: text("user_info").notNull(),
});

export const cacheEvents = sqliteTable("channel_cache_event", {
  singleton: integer("singleton").primaryKey(),
  sequence: integer("sequence").notNull(),
  event: text("event").notNull(),
  data: text("data").notNull(),
  expiresAt: integer("expires_at").notNull(),
});

export const outboxEvents = sqliteTable("channel_outbox", {
  sequence: integer("sequence").primaryKey(),
  event: text("event").notNull(),
  data: text("data").notNull(),
  excludedSocketId: text("excluded_socket_id"),
  userId: text("user_id"),
  createdAt: integer("created_at").notNull(),
});

export const branchDeliveries = sqliteTable(
  "channel_branch_deliveries",
  {
    sequence: integer("sequence").notNull(),
    branchName: text("branch_name").notNull(),
  },
  (table) => [primaryKey({ columns: [table.sequence, table.branchName] })],
);

export const fanoutMetadata = sqliteTable("fanout_metadata", {
  singleton: integer("singleton").primaryKey(),
  appId: text("app_id").notNull(),
  channel: text("channel").notNull(),
  branchName: text("branch_name").notNull(),
});

export const fanoutState = sqliteTable("fanout_state", {
  singleton: integer("singleton").primaryKey(),
  generation: integer("generation").notNull(),
});

export const fanoutGateways = sqliteTable("fanout_gateways", {
  gatewayName: text("gateway_name").primaryKey(),
  subscriptionCount: integer("subscription_count").notNull(),
  generation: integer("generation").notNull(),
});

export const directoryChannels = sqliteTable("directory_channels", {
  channel: text("channel").primaryKey(),
  subscriptionCount: integer("subscription_count").notNull(),
  userCount: integer("user_count").notNull(),
  updatedAt: integer("updated_at").notNull(),
});

export const userGateways = sqliteTable("user_gateways", {
  gatewayName: text("gateway_name").primaryKey(),
  connectionCount: integer("connection_count").notNull(),
  generation: integer("generation").notNull(),
});
