import * as Drizzle from "alchemy/Drizzle/Cloudflare";
import { bytesToHex } from "@noble/hashes/utils.js";
import { asc, eq, ne } from "drizzle-orm";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import {
  ApplicationBootstrap,
  ApplicationAuthorityState,
  ApplicationCreate,
  ApplicationSummary,
  ControlledApplication,
  ProvisionedApplication,
  ResolvedApplication,
  RuntimeApplication,
  type ApplicationBootstrapEncoded,
  type ApplicationAuthorityState as ApplicationAuthorityStateType,
  type ApplicationCreateEncoded,
  type ApplicationPatchEncoded,
  type ResolvedApplicationEncoded,
} from "../apps/model.ts";
import registryMigrations from "../db/registry-migrations.ts";
import { applications } from "../db/registry-schema.ts";
import { sha256Hex } from "../pusher/crypto.ts";
import { ActorError, mapActorError } from "../pusher/protocol.ts";
import { AppRegistry } from "./contracts.ts";
import { AppRegistryDependencies } from "./dependencies.ts";

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

const randomHex = Effect.fn("AppRegistry.randomHex")((byteLength: number) =>
  Effect.sync(() => bytesToHex(crypto.getRandomValues(new Uint8Array(byteLength)))),
);

const encodeApplicationSummary = Schema.encodeEffect(ApplicationSummary);
const encodeApplicationAuthorityState = Schema.encodeEffect(ApplicationAuthorityState);
const encodeProvisionedApplication = Schema.encodeEffect(ProvisionedApplication);
const encodeRuntimeApplication = Schema.encodeEffect(RuntimeApplication);

const randomEncryptionKey = Effect.fn("AppRegistry.randomEncryptionKey")(function* () {
  const bytes = yield* Effect.sync(() => crypto.getRandomValues(new Uint8Array(32)));
  return yield* Schema.encodeEffect(Schema.Uint8ArrayFromBase64)(bytes);
});

export const AppRegistryLive = AppRegistry.make(
  Effect.gen(function* () {
    const { authorities } = yield* AppRegistryDependencies;
    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.gen(function* () {
      const db = yield* Drizzle.DurableObject({ migrations: registryMigrations });
      const registryLock = yield* Semaphore.make(1);

      const rowById = Effect.fn("AppRegistry.rowById")(function* (appId: string) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.appId, appId))
          .limit(1);
        return Option.fromNullishOr(row);
      }, operationError("rowById"));

      const initializeAuthority = Effect.fn("AppRegistry.initializeAuthority")(function* (
        application: ApplicationAuthorityStateType,
      ) {
        return yield* authorities
          .getByName(application.appKey)
          .initialize(
            yield* encodeApplicationAuthorityState(application).pipe(
              Effect.mapError(() =>
                actorError("initializeAuthority", "Application could not be encoded"),
              ),
            ),
          );
      }, operationError("initializeAuthority"));

      const encodeRuntime = Effect.fn("AppRegistry.encodeRuntime")(function* (
        encoded: ResolvedApplicationEncoded,
        operation: string,
      ) {
        const application = yield* Schema.decodeEffect(ResolvedApplication)(encoded).pipe(
          Effect.mapError(() => actorError(operation, "Application could not be decoded")),
        );
        return yield* encodeRuntimeApplication({
          appId: application.appId,
          appKey: application.appKey,
          appSecret: application.appSecret,
          createdAt: application.createdAt,
          encryptionMasterKey: application.encryptionMasterKey,
          jurisdiction: application.jurisdiction,
          locationHint: application.locationHint,
          name: application.name,
          status: application.status,
          updatedAt: application.updatedAt,
        }).pipe(Effect.mapError(() => actorError(operation, "Application could not be encoded")));
      });

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
        const row: ApplicationRow = {
          appId,
          appKey,
          authTokenHash: sha256Hex(authToken),
          createdAt: now,
          jurisdiction: Option.getOrNull(decoded.jurisdiction),
          locationHint: Option.getOrNull(decoded.locationHint),
          name: decoded.name,
          status: "active",
          updatedAt: now,
        };
        const authorityState: ApplicationAuthorityStateType = {
          ...toSummary(row),
          appSecret,
          authTokenHash: row.authTokenHash,
          encryptionMasterKey,
          generation: 0,
        };
        const authority = authorities.getByName(row.appKey);
        yield* initializeAuthority(authorityState);
        const inserted = yield* db.insert(applications).values(row).pipe(Effect.result);
        if (Result.isFailure(inserted)) {
          yield* authority.remove().pipe(Effect.result);
          return yield* inserted.failure;
        }
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
        if (Option.isSome(existing) && existing.value.appKey !== decoded.appKey) {
          return yield* actorError("bootstrap", "Bootstrap application identity does not match");
        }
        const now = yield* Clock.currentTimeMillis;
        const row: ApplicationRow = Option.getOrElse(existing, () => ({
          appId: decoded.appId,
          appKey: decoded.appKey,
          authTokenHash: sha256Hex(decoded.authToken),
          createdAt: now,
          jurisdiction: Option.getOrNull(decoded.jurisdiction),
          locationHint: Option.getOrNull(decoded.locationHint),
          name: decoded.name,
          status: "active",
          updatedAt: now,
        }));
        const initialized = yield* initializeAuthority({
          ...toSummary(row),
          appSecret: decoded.appSecret,
          authTokenHash: row.authTokenHash,
          encryptionMasterKey: decoded.encryptionMasterKey,
          generation: 0,
        });
        if (Option.isNone(existing)) {
          const application = yield* Schema.decodeEffect(ResolvedApplication)(initialized).pipe(
            Effect.mapError(() => actorError("bootstrap", "Application could not be decoded")),
          );
          const inserted = yield* db
            .insert(applications)
            .values({
              appId: application.appId,
              appKey: application.appKey,
              authTokenHash: row.authTokenHash,
              createdAt: application.createdAt,
              jurisdiction: Option.getOrNull(application.jurisdiction),
              locationHint: Option.getOrNull(application.locationHint),
              name: application.name,
              status: application.status,
              updatedAt: application.updatedAt,
            })
            .pipe(Effect.result);
          if (Result.isFailure(inserted)) {
            return yield* inserted.failure;
          }
        }
      }, operationError("bootstrap"));

      const get = Effect.fn("AppRegistry.get")(function* (appId: string) {
        const row = yield* rowById(appId);
        if (Option.isNone(row)) {
          return null;
        }
        const application = yield* authorities.getByName(row.value.appKey).get();
        if (application === null && row.value.status !== "deleted") {
          yield* db
            .update(applications)
            .set({
              status: "deleted",
              updatedAt: yield* Clock.currentTimeMillis,
            })
            .where(eq(applications.appId, appId))
            .pipe(Effect.result);
        }
        return application;
      });

      const control = Effect.fn("AppRegistry.control")(function* (appId: string) {
        const row = yield* rowById(appId);
        if (Option.isNone(row)) {
          return null;
        }
        return yield* authorities.getByName(row.value.appKey).control();
      }, operationError("control"));

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

      const resolveByAuthToken = Effect.fn("AppRegistry.resolveByAuthToken")(function* (
        authToken: string,
      ) {
        const [row] = yield* db
          .select()
          .from(applications)
          .where(eq(applications.authTokenHash, sha256Hex(authToken)))
          .limit(1);
        const application = Option.fromNullishOr(row);
        if (Option.isNone(application)) {
          return null;
        }
        const resolved = Option.fromNullishOr(
          yield* authorities.getByName(application.value.appKey).authenticate(authToken),
        );
        return Option.isNone(resolved)
          ? null
          : yield* encodeRuntime(resolved.value, "resolveByAuthToken");
      }, operationError("resolveByAuthToken"));

      const update = Effect.fn("AppRegistry.update")(function* (
        appId: string,
        patch: ApplicationPatchEncoded,
      ) {
        const current = yield* rowById(appId);
        if (Option.isNone(current) || current.value.status === "deleted") {
          return yield* actorError("update", "Application does not exist");
        }
        const encoded = yield* authorities.getByName(current.value.appKey).update(patch);
        const updated = yield* Schema.decodeEffect(ControlledApplication)(encoded).pipe(
          Effect.mapError(() => actorError("update", "Application could not be decoded")),
        );
        const projected = yield* db
          .update(applications)
          .set({
            name: updated.name,
            status: updated.status,
            updatedAt: updated.updatedAt,
          })
          .where(eq(applications.appId, appId))
          .pipe(Effect.result);
        if (Result.isFailure(projected)) {
          yield* Effect.logWarning("Application directory projection update failed").pipe(
            Effect.annotateLogs({ appId, operation: "update" }),
          );
        }
        return encoded;
      }, operationError("update"));

      const remove = Effect.fn("AppRegistry.remove")(function* (appId: string) {
        const current = yield* rowById(appId);
        if (Option.isNone(current)) {
          return null;
        }
        const generation = yield* authorities.getByName(current.value.appKey).remove();
        if (generation === null) {
          return null;
        }
        const updatedAt = yield* Clock.currentTimeMillis;
        const projected = yield* db
          .update(applications)
          .set({
            status: "deleted",
            updatedAt,
          })
          .where(eq(applications.appId, appId))
          .pipe(Effect.result);
        if (Result.isFailure(projected)) {
          yield* Effect.logWarning("Application directory projection update failed").pipe(
            Effect.annotateLogs({ appId, operation: "remove" }),
          );
        }
        return generation;
      }, operationError("remove"));

      return {
        bootstrap: (input: ApplicationBootstrapEncoded) =>
          bootstrap(input).pipe(Semaphore.withPermit(registryLock)),
        control: (appId: string) => control(appId).pipe(Semaphore.withPermit(registryLock)),
        create: (input: ApplicationCreateEncoded) =>
          create(input).pipe(Semaphore.withPermit(registryLock)),
        get: (appId: string) => get(appId).pipe(Semaphore.withPermit(registryLock)),
        list: () => list().pipe(Semaphore.withPermit(registryLock)),
        remove: (appId: string) => remove(appId).pipe(Semaphore.withPermit(registryLock)),
        resolveByAuthToken: (authToken: string) =>
          resolveByAuthToken(authToken).pipe(Semaphore.withPermit(registryLock)),
        update: (appId: string, patch: ApplicationPatchEncoded) =>
          update(appId, patch).pipe(Semaphore.withPermit(registryLock)),
      };
    });
  }),
);
