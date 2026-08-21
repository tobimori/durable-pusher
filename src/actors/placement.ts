import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ApplicationPlacement } from "../apps/model.ts";

export class PlacementError extends Schema.TaggedError<PlacementError>()("PlacementError", {
  cause: Schema.Defect(),
  message: Schema.String,
  namespace: Schema.String,
}) {}

export interface PlacedNamespace<Stub> {
  readonly getByName: (
    name: string,
    placement: ApplicationPlacement,
  ) => Effect.Effect<Stub, PlacementError>;
}

interface NativeRestrictedNamespace {
  readonly getByName: (
    name: string,
    options?: DurableObjectNamespaceGetDurableObjectOptions,
  ) => unknown;
}

interface NativeNamespace {
  readonly jurisdiction: (jurisdiction: DurableObjectJurisdiction) => NativeRestrictedNamespace;
}

export const makePlacedNamespace = <Stub>(
  bindingName: string,
  namespace: {
    readonly getByName: (
      name: string,
      options?: DurableObjectNamespaceGetDurableObjectOptions,
    ) => Stub;
  },
  environment: Readonly<Record<string, NativeNamespace>>,
): PlacedNamespace<Stub> => ({
  getByName: (name, placement) =>
    Effect.gen(function* () {
      const locationHint = placement.locationHint;
      const jurisdiction = placement.jurisdiction;
      return yield* Effect.try({
        try: () => {
          if (Option.isNone(jurisdiction)) {
            return Option.isNone(locationHint)
              ? namespace.getByName(name)
              : namespace.getByName(name, { locationHint: locationHint.value });
          }
          const nativeNamespace = Option.getOrThrow(Option.fromNullishOr(environment[bindingName]));
          const restricted = nativeNamespace.jurisdiction(jurisdiction.value);
          const stub = Option.isNone(locationHint)
            ? restricted.getByName(name)
            : restricted.getByName(name, { locationHint: locationHint.value });
          return Cloudflare.makeRpcStub<Stub>(stub);
        },
        catch: (cause) =>
          new PlacementError({
            cause,
            message: `Could not route ${bindingName} in the requested placement`,
            namespace: bindingName,
          }),
      });
    }),
});
