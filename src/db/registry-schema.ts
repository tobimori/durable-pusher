import { integer, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core";

export const applications = sqliteTable(
  "applications",
  {
    appId: text("app_id").primaryKey(),
    appKey: text("app_key").notNull(),
    appSecret: text("app_secret").notNull(),
    authTokenHash: text("auth_token_hash").notNull(),
    encryptionMasterKey: text("encryption_master_key").notNull(),
    name: text("name").notNull(),
    status: text("status", { enum: ["active", "disabled", "deleted"] }).notNull(),
    jurisdiction: text("jurisdiction", {
      enum: ["eu", "fedramp", "fedramp-high", "us"],
    }),
    locationHint: text("location_hint", {
      enum: ["afr", "apac", "apac-ne", "apac-se", "eeur", "enam", "me", "oc", "sam", "weur", "wnam"],
    }),
    createdAt: integer("created_at").notNull(),
    updatedAt: integer("updated_at").notNull(),
  },
  (table) => [
    uniqueIndex("applications_app_key").on(table.appKey),
    uniqueIndex("applications_auth_token_hash").on(table.authTokenHash),
  ],
);
