export default {
  migrations: {
    "0000_initial": `
      CREATE TABLE connection_sockets (socket_id TEXT PRIMARY KEY NOT NULL, app_id TEXT NOT NULL, protocol INTEGER NOT NULL, shard_name TEXT NOT NULL, user_id TEXT, user_data TEXT, event_window INTEGER NOT NULL, event_count INTEGER NOT NULL);
      CREATE INDEX connection_sockets_user_id ON connection_sockets (user_id);
      CREATE TABLE connection_subscriptions (socket_id TEXT NOT NULL, channel TEXT NOT NULL, branch_name TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL, start_sequence INTEGER NOT NULL, last_sequence INTEGER NOT NULL, user_id TEXT, user_info TEXT, PRIMARY KEY (socket_id, channel));
      CREATE INDEX connection_subscriptions_channel ON connection_subscriptions (channel, state);
      CREATE TABLE connection_channel_versions (channel TEXT PRIMARY KEY NOT NULL, generation INTEGER NOT NULL);
      CREATE TABLE connection_user_versions (user_id TEXT PRIMARY KEY NOT NULL, generation INTEGER NOT NULL);
      CREATE TABLE connection_pending_events (socket_id TEXT NOT NULL, channel TEXT NOT NULL, sequence INTEGER NOT NULL, event TEXT NOT NULL, data TEXT NOT NULL, user_id TEXT, PRIMARY KEY (socket_id, channel, sequence));
      CREATE TABLE channel_metadata (singleton INTEGER PRIMARY KEY NOT NULL, app_id TEXT NOT NULL, channel TEXT NOT NULL);
      CREATE TABLE channel_state (singleton INTEGER PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL);
      CREATE TABLE channel_branches (branch_name TEXT PRIMARY KEY NOT NULL, subscription_count INTEGER NOT NULL, generation INTEGER NOT NULL);
      CREATE TABLE channel_presence (socket_id TEXT PRIMARY KEY NOT NULL, branch_name TEXT NOT NULL, user_id TEXT NOT NULL, user_info TEXT NOT NULL);
      CREATE INDEX channel_presence_user_id ON channel_presence (user_id);
      CREATE TABLE channel_cache_event (singleton INTEGER PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL, event TEXT NOT NULL, data TEXT NOT NULL, expires_at INTEGER NOT NULL);
      CREATE TABLE channel_outbox (sequence INTEGER PRIMARY KEY NOT NULL, event TEXT NOT NULL, data TEXT NOT NULL, excluded_socket_id TEXT, user_id TEXT, created_at INTEGER NOT NULL);
      CREATE TABLE channel_branch_deliveries (sequence INTEGER NOT NULL, branch_name TEXT NOT NULL, PRIMARY KEY (sequence, branch_name));
      CREATE TABLE fanout_metadata (singleton INTEGER PRIMARY KEY NOT NULL, app_id TEXT NOT NULL, channel TEXT NOT NULL);
      CREATE TABLE fanout_state (singleton INTEGER PRIMARY KEY NOT NULL, generation INTEGER NOT NULL);
      CREATE TABLE fanout_gateways (gateway_name TEXT PRIMARY KEY NOT NULL, subscription_count INTEGER NOT NULL, generation INTEGER NOT NULL);
      CREATE TABLE directory_channels (channel TEXT PRIMARY KEY NOT NULL, subscription_count INTEGER NOT NULL, user_count INTEGER NOT NULL, updated_at INTEGER NOT NULL);
      CREATE TABLE user_gateways (gateway_name TEXT PRIMARY KEY NOT NULL, connection_count INTEGER NOT NULL, generation INTEGER NOT NULL);
    `,
    "0001_fanout_branch_name": `
      ALTER TABLE fanout_metadata ADD COLUMN branch_name TEXT NOT NULL DEFAULT '';
    `,
    "0002_application_placement": `
      ALTER TABLE channel_metadata ADD COLUMN jurisdiction TEXT;
      ALTER TABLE channel_metadata ADD COLUMN location_hint TEXT;
      ALTER TABLE fanout_metadata ADD COLUMN jurisdiction TEXT;
      ALTER TABLE fanout_metadata ADD COLUMN location_hint TEXT;
      CREATE TABLE user_metadata (singleton INTEGER PRIMARY KEY NOT NULL, app_id TEXT NOT NULL, user_id TEXT NOT NULL, jurisdiction TEXT, location_hint TEXT);
    `,
    "0003_direct_gateways_and_catalog": `
      CREATE TABLE channel_gateways (gateway_name TEXT PRIMARY KEY NOT NULL);
      CREATE TABLE connection_shard_catalog (singleton INTEGER PRIMARY KEY NOT NULL, app_id TEXT NOT NULL, jurisdiction TEXT, location_hint TEXT, shard_count INTEGER NOT NULL);
    `,
    "0004_remove_persistent_realtime_state": `
      SELECT 1;
    `,
    "0005_gateway_registration_tokens": `
      ALTER TABLE channel_gateways ADD COLUMN registration_token TEXT NOT NULL DEFAULT '';
      DELETE FROM channel_gateways;
      DELETE FROM directory_channels;
    `,
  },
};
