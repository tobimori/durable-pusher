import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { bytesToHex } from "@noble/hashes/utils.js";
import { asc, eq, ne } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  ApplicationBootstrap,
  ApplicationCreate,
  ApplicationPatch,
  type ApplicationSummary,
  type ProvisionedApplication,
  type RuntimeApplication,
} from "../apps/model.ts";
import registryMigrations from "../db/registry-migrations.ts";
import { applications } from "../db/registry-schema.ts";
import { sha256Hex } from "../pusher/crypto.ts";
import { ActorError, mapActorError } from "../pusher/protocol.ts";
import { AppRegistry } from "./contracts.ts";

export { AppRegistry };

type ApplicationRow = typeof applications.$inferSelect;

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "AppRegistry", message, operation });

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("AppRegistry", operation));

const toSummary = (row: ApplicationRow): ApplicationSummary => ({
  appId: row.appId,
  appKey: row.appKey,
  createdAt: row.createdAt,
  jurisdiction: row.jurisdiction,
  locationHint: row.locationHint,
  name: row.name,
  status: row.status,
  updatedAt: row.updatedAt,
});

const toRuntime = (row: ApplicationRow): RuntimeApplication => ({
  ...toSummary(row),
  appSecret: row.appSecret,
  encryptionMasterKey: row.encryptionMasterKey,
});

const randomHex = Effect.fn("AppRegistry.randomHex")((byteLength: number) =>
  Effect.sync(() => bytesToHex(crypto.getRandomValues(new Uint8Array(byteLength)))),
);

const randomEncryptionKey = Effect.fn("AppRegistry.randomEncryptionKey")(function* () {
  const bytes = yield* Effect.sync(() => crypto.getRandomValues(new Uint8Array(32)));
  return yield* Schema.encodeEffect(Schema.Uint8ArrayFromBase64)(bytes);
});

export const AppRegistryLive = AppRegistry.make(
  Effect.succeed(
    Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations: registryMigrations });

      const rowById = Effect.fn("AppRegistry.rowById")(function* (appId: string) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.appId, appId))
          .limit(1);
        return row;
      }, operationError("rowById"));

      const create = Effect.fn("AppRegistry.create")(function* (input: ApplicationCreate) {
        const decoded = yield* Schema.decodeEffect(ApplicationCreate)(input).pipe(
          Effect.mapError(() => actorError("create", "Invalid application settings")),
        );
        const appId = decoded.appId ?? (yield* randomHex(8));
        const appKey = yield* randomHex(10);
        const appSecret = yield* randomHex(16);
        const authToken = yield* randomHex(24);
        const encryptionMasterKey = yield* randomEncryptionKey();
        const now = yield* Clock.currentTimeMillis;
        yield* db.insert(applications).values({
          appId,
          appKey,
          appSecret,
          authTokenHash: sha256Hex(authToken),
          createdAt: now,
          encryptionMasterKey,
          jurisdiction: decoded.jurisdiction ?? null,
          locationHint: decoded.locationHint ?? null,
          name: decoded.name,
          status: "active",
          updatedAt: now,
        });
        const provisioned: ProvisionedApplication = {
          appId,
          appKey,
          appSecret,
          authToken,
          createdAt: now,
          encryptionMasterKey,
          jurisdiction: decoded.jurisdiction ?? null,
          locationHint: decoded.locationHint ?? null,
          name: decoded.name,
          status: "active",
          updatedAt: now,
        };
        return provisioned;
      }, operationError("create"));

      const bootstrap = Effect.fn("AppRegistry.bootstrap")(function* (
        input: ApplicationBootstrap,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationBootstrap)(input).pipe(
          Effect.mapError(() => actorError("bootstrap", "Invalid bootstrap application")),
        );
        const existing = yield* rowById(decoded.appId);
        if (existing !== undefined) {
          return toRuntime(existing);
        }
        const now = yield* Clock.currentTimeMillis;
        yield* db.insert(applications).values({
          appId: decoded.appId,
          appKey: decoded.appKey,
          appSecret: decoded.appSecret,
          authTokenHash: sha256Hex(decoded.authToken),
          createdAt: now,
          encryptionMasterKey: decoded.encryptionMasterKey,
          jurisdiction: decoded.jurisdiction ?? null,
          locationHint: decoded.locationHint ?? null,
          name: decoded.name,
          status: "active",
          updatedAt: now,
        });
        const created = yield* rowById(decoded.appId);
        if (created === undefined) {
          return yield* actorError("bootstrap", "Bootstrap application was not persisted");
        }
        return toRuntime(created);
      }, operationError("bootstrap"));

      const get = Effect.fn("AppRegistry.get")(function* (appId: string) {
        const row = yield* rowById(appId);
        return row === undefined || row.status === "deleted" ? null : toSummary(row);
      });

      const list = Effect.fn("AppRegistry.list")(function* () {
        const rows = yield* db
          .select()
          .from(applications)
          .where(ne(applications.status, "deleted"))
          .orderBy(asc(applications.createdAt), asc(applications.appId));
        return rows.map(toSummary);
      }, operationError("list"));

      const resolveById = Effect.fn("AppRegistry.resolveById")(function* (appId: string) {
        const row = yield* rowById(appId);
        return row?.status === "active" ? toRuntime(row) : null;
      });

      const resolveByKey = Effect.fn("AppRegistry.resolveByKey")(function* (appKey: string) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.appKey, appKey))
          .limit(1);
        return row?.status === "active" ? toRuntime(row) : null;
      }, operationError("resolveByKey"));

      const resolveByAuthToken = Effect.fn("AppRegistry.resolveByAuthToken")(function* (
        authToken: string,
      ) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.authTokenHash, sha256Hex(authToken)))
          .limit(1);
        return row?.status === "active" ? toRuntime(row) : null;
      }, operationError("resolveByAuthToken"));

      const update = Effect.fn("AppRegistry.update")(function* (
        appId: string,
        patch: ApplicationPatch,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPatch)(patch).pipe(
          Effect.mapError(() => actorError("update", "Invalid application settings")),
        );
        const current = yield* rowById(appId);
        if (current === undefined || current.status === "deleted") {
          return yield* actorError("update", "Application does not exist");
        }
        const updatedAt = yield* Clock.currentTimeMillis;
        yield* db
          .update(applications)
          .set({
            jurisdiction:
              "jurisdiction" in decoded ? decoded.jurisdiction : current.jurisdiction,
            locationHint:
              "locationHint" in decoded ? decoded.locationHint : current.locationHint,
            name: decoded.name ?? current.name,
            status:
              decoded.enabled === undefined
                ? current.status
                : decoded.enabled
                  ? "active"
                  : "disabled",
            updatedAt,
          })
          .where(eq(applications.appId, appId));
        const updated = yield* rowById(appId);
        if (updated === undefined) {
          return yield* actorError("update", "Application update was not persisted");
        }
        return toSummary(updated);
      }, operationError("update"));

      const remove = Effect.fn("AppRegistry.remove")(function* (appId: string) {
        const current = yield* rowById(appId);
        if (current === undefined || current.status === "deleted") {
          return false;
        }
        const updatedAt = yield* Clock.currentTimeMillis;
        yield* db
          .update(applications)
          .set({
            appSecret: "",
            encryptionMasterKey: "",
            status: "deleted",
            updatedAt,
          })
          .where(eq(applications.appId, appId));
        return true;
      }, operationError("remove"));

      return {
        bootstrap,
        create,
        get,
        list,
        remove,
        resolveByAuthToken,
        resolveById,
        resolveByKey,
        update,
      };
    }),
  ),
);
