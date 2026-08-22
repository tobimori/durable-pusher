import * as Cloudflare from "alchemy/Cloudflare";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import {
  ApplicationAuthorityState,
  ApplicationPatch,
  ApplicationSummary,
  ControlledApplication,
  ResolvedApplication,
  type ApplicationAuthorityState as ApplicationAuthorityStateType,
  type ApplicationAuthorityStateEncoded,
  type ApplicationPatchEncoded,
  type ApplicationSummary as ApplicationSummaryType,
  type ResolvedApplication as ResolvedApplicationType,
} from "../apps/model.ts";
import { sha256Hex, timingSafeEqual } from "../pusher/crypto.ts";
import { ActorError, mapActorError } from "../pusher/protocol.ts";
import { ApplicationAuthority } from "./contracts.ts";

export { ApplicationAuthority };

const STATE_KEY = "application";

const actorError = (operation: string, message: string): ActorError =>
  ActorError.make({ actor: "ApplicationAuthority", message, operation });

const operationError = (operation: string) =>
  Effect.mapError(mapActorError("ApplicationAuthority", operation));

const toSummary = (state: ApplicationAuthorityStateType): ApplicationSummaryType => ({
  appId: state.appId,
  appKey: state.appKey,
  createdAt: state.createdAt,
  jurisdiction: state.jurisdiction,
  locationHint: state.locationHint,
  name: state.name,
  status: state.status,
  updatedAt: state.updatedAt,
});

const toResolved = (state: ApplicationAuthorityStateType): ResolvedApplicationType => ({
  ...toSummary(state),
  appSecret: state.appSecret,
  encryptionMasterKey: state.encryptionMasterKey,
  generation: state.generation,
});

const toControlled = (state: ApplicationAuthorityStateType) => ({
  ...toSummary(state),
  generation: state.generation,
});

const encodeSummary = Schema.encodeEffect(ApplicationSummary);
const encodeControlledApplication = Schema.encodeEffect(ControlledApplication);
const encodeResolved = Schema.encodeEffect(ResolvedApplication);
const encodeState = Schema.encodeEffect(ApplicationAuthorityState);
const decodeState = Schema.decodeUnknownEffect(ApplicationAuthorityState);

export const ApplicationAuthorityLive = ApplicationAuthority.make(
  Effect.succeed(
    Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      const authorityLock = yield* Semaphore.make(1);
      let cachedApplication: ApplicationAuthorityStateType | undefined;
      let stateLoaded = false;

      const read = Effect.fn("ApplicationAuthority.read")(function* () {
        if (stateLoaded) {
          return Option.fromNullishOr(cachedApplication);
        }
        const stored = Option.fromNullishOr(yield* state.storage.get<unknown>(STATE_KEY));
        cachedApplication = Option.isNone(stored)
          ? undefined
          : yield* decodeState(stored.value).pipe(
              Effect.mapError(() => actorError("read", "Application state is invalid")),
            );
        stateLoaded = true;
        return Option.fromNullishOr(cachedApplication);
      }, operationError("read"));

      const write = Effect.fn("ApplicationAuthority.write")(function* (
        application: ApplicationAuthorityStateType,
      ) {
        yield* state.storage.put(
          STATE_KEY,
          yield* encodeState(application).pipe(
            Effect.mapError(() => actorError("write", "Application state could not be encoded")),
          ),
        );
        cachedApplication = application;
        stateLoaded = true;
      }, operationError("write"));

      const initialize = Effect.fn("ApplicationAuthority.initialize")(function* (
        input: ApplicationAuthorityStateEncoded,
      ) {
        const requested = yield* Schema.decodeEffect(ApplicationAuthorityState)(input).pipe(
          Effect.mapError(() => actorError("initialize", "Invalid application state")),
        );
        const current = yield* read();
        if (Option.isSome(current)) {
          if (
            current.value.appId !== requested.appId ||
            current.value.appKey !== requested.appKey
          ) {
            return yield* actorError("initialize", "Application identity does not match");
          }
          return yield* encodeResolved(toResolved(current.value)).pipe(
            Effect.mapError(() => actorError("initialize", "Application could not be encoded")),
          );
        }
        yield* write(requested);
        return yield* encodeResolved(toResolved(requested)).pipe(
          Effect.mapError(() => actorError("initialize", "Application could not be encoded")),
        );
      }, operationError("initialize"));

      const get = Effect.fn("ApplicationAuthority.get")(function* () {
        const current = yield* read();
        if (Option.isNone(current) || current.value.status === "deleted") {
          return null;
        }
        return yield* encodeSummary(toSummary(current.value)).pipe(
          Effect.mapError(() => actorError("get", "Application could not be encoded")),
        );
      });

      const control = Effect.fn("ApplicationAuthority.control")(function* () {
        const current = yield* read();
        if (Option.isNone(current)) {
          return null;
        }
        return yield* encodeControlledApplication(toControlled(current.value)).pipe(
          Effect.mapError(() => actorError("control", "Application could not be encoded")),
        );
      });

      const resolve = Effect.fn("ApplicationAuthority.resolve")(function* () {
        const current = yield* read();
        if (Option.isNone(current) || current.value.status !== "active") {
          return null;
        }
        return yield* encodeResolved(toResolved(current.value)).pipe(
          Effect.mapError(() => actorError("resolve", "Application could not be encoded")),
        );
      });

      const authenticate = Effect.fn("ApplicationAuthority.authenticate")(function* (
        authToken: string,
      ) {
        const current = yield* read();
        if (
          Option.isNone(current) ||
          current.value.status !== "active" ||
          !timingSafeEqual(current.value.authTokenHash, sha256Hex(authToken))
        ) {
          return null;
        }
        return yield* encodeResolved(toResolved(current.value)).pipe(
          Effect.mapError(() => actorError("authenticate", "Application could not be encoded")),
        );
      });

      const update = Effect.fn("ApplicationAuthority.update")(function* (
        patch: ApplicationPatchEncoded,
      ) {
        const decoded = yield* Schema.decodeEffect(ApplicationPatch)(patch).pipe(
          Effect.mapError(() => actorError("update", "Invalid application settings")),
        );
        const current = yield* read();
        if (Option.isNone(current) || current.value.status === "deleted") {
          return yield* actorError("update", "Application does not exist");
        }
        const updated: ApplicationAuthorityStateType = {
          ...current.value,
          generation: current.value.generation + 1,
          name: Option.getOrElse(decoded.name, () => current.value.name),
          status: Option.match(decoded.enabled, {
            onNone: () => current.value.status,
            onSome: (enabled) => (enabled ? "active" : "disabled"),
          }),
          updatedAt: yield* Clock.currentTimeMillis,
        };
        yield* write(updated);
        return yield* encodeControlledApplication(toControlled(updated)).pipe(
          Effect.mapError(() => actorError("update", "Application could not be encoded")),
        );
      }, operationError("update"));

      const remove = Effect.fn("ApplicationAuthority.remove")(function* () {
        const current = yield* read();
        if (Option.isNone(current)) {
          return null;
        }
        if (current.value.status === "deleted") {
          return current.value.generation;
        }
        const generation = current.value.generation + 1;
        yield* write({
          ...current.value,
          appSecret: "",
          encryptionMasterKey: "",
          generation,
          status: "deleted",
          updatedAt: yield* Clock.currentTimeMillis,
        });
        return generation;
      }, operationError("remove"));

      return {
        authenticate,
        control,
        get,
        initialize: (input: ApplicationAuthorityStateEncoded) =>
          initialize(input).pipe(Semaphore.withPermit(authorityLock)),
        remove: () => remove().pipe(Semaphore.withPermit(authorityLock)),
        resolve,
        update: (patch: ApplicationPatchEncoded) =>
          update(patch).pipe(Semaphore.withPermit(authorityLock)),
      };
    }),
  ),
);
