export default {
  migrations: {
    "0000_registry": `
      CREATE TABLE applications (app_id TEXT PRIMARY KEY NOT NULL, app_key TEXT NOT NULL, auth_token_hash TEXT NOT NULL, name TEXT NOT NULL, status TEXT NOT NULL, jurisdiction TEXT, location_hint TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
      CREATE UNIQUE INDEX applications_app_key ON applications (app_key);
      CREATE UNIQUE INDEX applications_auth_token_hash ON applications (auth_token_hash);
    `,
  },
};
