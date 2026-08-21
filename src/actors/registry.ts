import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { bytesToHex } from "@noble/hashes/utils.js";
import { asc, eq, ne } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  ApplicationBootstrap,
  ApplicationCreate,
  ApplicationPatch,
  ApplicationSummary,
  ProvisionedApplication,
  RuntimeApplication,
  type ApplicationBootstrapEncoded,
  type ApplicationCreateEncoded,
  type ApplicationPatchEncoded,
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
  jurisdiction: Option.fromNullishOr(row.jurisdiction),
  locationHint: Option.fromNullishOr(row.locationHint),
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

const encodeApplicationSummary = Schema.encodeEffect(ApplicationSummary);
const encodeProvisionedApplication = Schema.encodeEffect(ProvisionedApplication);
const encodeRuntimeApplication = Schema.encodeEffect(RuntimeApplication);

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
        return Option.fromNullishOr(row);
      }, operationError("rowById"));

      const create = Effect.fn("AppRegistry.create")(function* (input: ApplicationCreateEncoded) {
        const decoded = yield* Schema.decodeEffect(ApplicationCreate)(input).pipe(
          Effect.mapError(() => actorError("create", "Invalid application settings")),
        );
        if (Option.isSome(decoded.appId) && Option.isSome(yield* rowById(decoded.appId.value))) {
          return yield* actorError("create", "Application ID is not available");
        }
        const appId = Option.isSome(decoded.appId) ? decoded.appId.value : yield* randomHex(8);
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
          jurisdiction: Option.getOrNull(decoded.jurisdiction),
          locationHint: Option.getOrNull(decoded.locationHint),
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
          jurisdiction: decoded.jurisdiction,
          locationHint: decoded.locationHint,
          name: decoded.name,
          status: "active",
          updatedAt: now,
        };
        return yield* encodeProvisionedApplication(provisioned).pipe(
          Effect.mapError(() => actorError("create", "Application could not be encoded")),
        );
      }, operationError("create"));

      const bootstrap = Effect.fn("AppRegistry.bootstrap")(function* (
        input: ApplicationBootstrapEncoded,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationBootstrap)(input).pipe(
          Effect.mapError(() => actorError("bootstrap", "Invalid bootstrap application")),
        );
        const existing = yield* rowById(decoded.appId);
        if (Option.isSome(existing)) {
          return yield* encodeRuntimeApplication(toRuntime(existing.value)).pipe(
            Effect.mapError(() => actorError("bootstrap", "Application could not be encoded")),
          );
        }
        const now = yield* Clock.currentTimeMillis;
        yield* db.insert(applications).values({
          appId: decoded.appId,
          appKey: decoded.appKey,
          appSecret: decoded.appSecret,
          authTokenHash: sha256Hex(decoded.authToken),
          createdAt: now,
          encryptionMasterKey: decoded.encryptionMasterKey,
          jurisdiction: Option.getOrNull(decoded.jurisdiction),
          locationHint: Option.getOrNull(decoded.locationHint),
          name: decoded.name,
          status: "active",
          updatedAt: now,
        }).onConflictDoNothing();
        const created = yield* rowById(decoded.appId);
        if (Option.isNone(created)) {
          return yield* actorError("bootstrap", "Bootstrap application was not persisted");
        }
        return yield* encodeRuntimeApplication(toRuntime(created.value)).pipe(
          Effect.mapError(() => actorError("bootstrap", "Application could not be encoded")),
        );
      }, operationError("bootstrap"));

      const get = Effect.fn("AppRegistry.get")(function* (appId: string) {
        const row = yield* rowById(appId);
        const summary = Option.filter(row, (value) => value.status !== "deleted").pipe(
          Option.map(toSummary),
        );
        if (Option.isNone(summary)) {
          return null;
        }
        return yield* encodeApplicationSummary(summary.value).pipe(
          Effect.mapError(() => actorError("get", "Application could not be encoded")),
        );
      });

      const list = Effect.fn("AppRegistry.list")(function* () {
        const rows = yield* db
          .select()
          .from(applications)
          .where(ne(applications.status, "deleted"))
          .orderBy(asc(applications.createdAt), asc(applications.appId));
        return yield* Effect.forEach(rows, (row) =>
          encodeApplicationSummary(toSummary(row)).pipe(
            Effect.mapError(() => actorError("list", "Application could not be encoded")),
          ),
        );
      }, operationError("list"));

      const resolveById = Effect.fn("AppRegistry.resolveById")(function* (appId: string) {
        const row = yield* rowById(appId);
        const application = Option.filter(row, (value) => value.status === "active").pipe(
          Option.map(toRuntime),
        );
        if (Option.isNone(application)) {
          return null;
        }
        return yield* encodeRuntimeApplication(application.value).pipe(
          Effect.mapError(() => actorError("resolveById", "Application could not be encoded")),
        );
      });

      const resolveByKey = Effect.fn("AppRegistry.resolveByKey")(function* (appKey: string) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.appKey, appKey))
          .limit(1);
        const application = Option.fromNullishOr(row).pipe(
          Option.filter((value) => value.status === "active"),
          Option.map(toRuntime),
        );
        if (Option.isNone(application)) {
          return null;
        }
        return yield* encodeRuntimeApplication(application.value).pipe(
          Effect.mapError(() => actorError("resolveByKey", "Application could not be encoded")),
        );
      }, operationError("resolveByKey"));

      const resolveByAuthToken = Effect.fn("AppRegistry.resolveByAuthToken")(function* (
        authToken: string,
      ) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.authTokenHash, sha256Hex(authToken)))
          .limit(1);
        const application = Option.fromNullishOr(row).pipe(
          Option.filter((value) => value.status === "active"),
          Option.map(toRuntime),
        );
        if (Option.isNone(application)) {
          return null;
        }
        return yield* encodeRuntimeApplication(application.value).pipe(
          Effect.mapError(() =>
            actorError("resolveByAuthToken", "Application could not be encoded"),
          ),
        );
      }, operationError("resolveByAuthToken"));

      const update = Effect.fn("AppRegistry.update")(function* (
        appId: string,
        patch: ApplicationPatchEncoded,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPatch)(patch).pipe(
          Effect.mapError(() => actorError("update", "Invalid application settings")),
        );
        const current = yield* rowById(appId);
        if (Option.isNone(current) || current.value.status === "deleted") {
          return yield* actorError("update", "Application does not exist");
        }
        const currentValue = current.value;
        const updatedAt = yield* Clock.currentTimeMillis;
        yield* db
          .update(applications)
          .set({
            name: Option.getOrElse(decoded.name, () => currentValue.name),
            status: Option.match(decoded.enabled, {
              onNone: () => currentValue.status,
              onSome: (enabled) => (enabled ? "active" : "disabled"),
            }),
            updatedAt,
          })
          .where(eq(applications.appId, appId));
        const updated = yield* rowById(appId);
        if (Option.isNone(updated)) {
          return yield* actorError("update", "Application update was not persisted");
        }
        return yield* encodeApplicationSummary(toSummary(updated.value)).pipe(
          Effect.mapError(() => actorError("update", "Application could not be encoded")),
        );
      }, operationError("update"));

      const remove = Effect.fn("AppRegistry.remove")(function* (appId: string) {
        const current = yield* rowById(appId);
        if (Option.isNone(current) || current.value.status === "deleted") {
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
