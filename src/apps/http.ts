import type { RuntimeContext } from "alchemy";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import type * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { AppRegistry } from "../actors/contracts.ts";
import { timingSafeEqual } from "../pusher/crypto.ts";
import { ApiError, type JsonValue } from "../pusher/protocol.ts";
import {
  ApplicationCreate,
  ApplicationPatch,
  ApplicationPlacement,
  ApplicationSummary,
  type ApplicationPlacementEncoded,
  type ApplicationSummaryEncoded,
} from "./model.ts";

type RegistryNamespace = Effect.Success<typeof AppRegistry>;
type ParsedUrl = typeof Schema.URLFromString.Type;
type ApplicationTerminator = (
  placement: ApplicationPlacementEncoded,
) => Effect.Effect<number, { readonly message: string }, RuntimeContext>;

const decodeJsonValue = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));
const decodePathSegment = Schema.decodeEffect(Schema.StringFromUriComponent);
const encodeJsonResponse = HttpServerResponse.schemaJson(Schema.Json);

const RESPONSE_HEADERS = {
  "access-control-allow-origin": "*",
  "cache-control": "no-store",
};

const apiError = (status: number, message: string): ApiError => ApiError.make({ message, status });

const json = (value: JsonValue, status = 200) =>
  encodeJsonResponse(value, {
    headers: RESPONSE_HEADERS,
    status,
  });

const registryUnavailable = Effect.mapError(() =>
  apiError(503, "Application registry is unavailable"),
);

const methodNotAllowed = Effect.fn("ApplicationsHttp.methodNotAllowed")(function* (allow: string) {
  const response = yield* json({ error: "Method not allowed" }, 405);
  return HttpServerResponse.setHeader(response, "allow", allow);
});

const decodeJsonBody = Effect.fn("ApplicationsHttp.decodeJsonBody")(function* <
  S extends Schema.Constraint,
>(schema: S, request: HttpServerRequest.HttpServerRequest) {
  const buffer = yield* request.arrayBuffer;
  const text = yield* Effect.sync(() => new TextDecoder().decode(buffer));
  const value = yield* decodeJsonValue(text).pipe(
    Effect.mapError(() => apiError(400, "Request body is not valid JSON")),
  );
  return yield* Schema.decodeEffect(schema, { onExcessProperty: "error" })(value).pipe(
    Effect.mapError(() => apiError(400, "Request body does not match the application API")),
  );
});

const applicationId = Effect.fn("ApplicationsHttp.applicationId")(function* (url: ParsedUrl) {
  const prefix = "/control/v1/apps/";
  if (!url.pathname.startsWith(prefix)) {
    return Option.none<string>();
  }
  const encoded = url.pathname.slice(prefix.length);
  if (encoded.length === 0 || encoded.includes("/")) {
    return Option.none<string>();
  }
  return Option.some(
    yield* decodePathSegment(encoded).pipe(
      Effect.mapError(() => apiError(400, "Application ID has invalid percent encoding")),
    ),
  );
});

export const makeApplicationsHttp = (
  applications: RegistryNamespace,
  controlToken: Redacted.Redacted<string>,
  terminateApplication: ApplicationTerminator,
) => {
  const terminate = Effect.fn("ApplicationsHttp.terminate")(function* (
    encoded: ApplicationSummaryEncoded,
  ) {
    const application = yield* Schema.decodeEffect(ApplicationSummary)(encoded).pipe(
      Effect.mapError(() => apiError(503, "Application settings could not be decoded")),
    );
    const placement = yield* Schema.encodeEffect(ApplicationPlacement)({
      appId: application.appId,
      jurisdiction: application.jurisdiction,
      locationHint: application.locationHint,
    }).pipe(Effect.mapError(() => apiError(503, "Application placement could not be encoded")));
    yield* terminateApplication(placement).pipe(
      Effect.mapError(() => apiError(503, "Application connections could not be terminated")),
    );
  });

  const authorize = Effect.fn("ApplicationsHttp.authorize")(function* (
    request: HttpServerRequest.HttpServerRequest,
  ) {
    const authorization = Option.fromNullishOr(request.headers.authorization);
    if (
      Option.isNone(authorization) ||
      !timingSafeEqual(authorization.value, `Bearer ${Redacted.value(controlToken)}`)
    ) {
      return yield* apiError(403, "Authorization denied");
    }
  });

  const handle = Effect.fn("ApplicationsHttp.handle")(function* (
    request: HttpServerRequest.HttpServerRequest,
    url: ParsedUrl,
  ) {
    yield* authorize(request);
    const registry = applications.getByName("applications");

    if (url.pathname === "/control/v1/apps") {
      if (request.method === "GET") {
        const apps = yield* registry.list().pipe(registryUnavailable);
        return yield* json({ apps });
      }
      if (request.method === "POST") {
        const input = yield* decodeJsonBody(ApplicationCreate, request);
        const encoded = yield* Schema.encodeEffect(ApplicationCreate)(input).pipe(
          Effect.mapError(() => apiError(400, "Application settings could not be encoded")),
        );
        const application = yield* registry
          .create(encoded)
          .pipe(
            Effect.mapError((error) =>
              error.message.includes("Application ID is not available")
                ? apiError(409, "Application ID is not available")
                : apiError(503, "Application registry is unavailable"),
            ),
          );
        return yield* json(application, 201);
      }
      return yield* methodNotAllowed("GET, POST");
    }

    const appId = yield* applicationId(url);
    if (Option.isNone(appId)) {
      return yield* apiError(404, "Control API endpoint not found");
    }
    if (request.method === "GET") {
      const application = Option.fromNullishOr(
        yield* registry.get(appId.value).pipe(registryUnavailable),
      );
      if (Option.isNone(application)) {
        return yield* apiError(404, "Application does not exist");
      }
      return yield* json(application.value);
    }
    if (request.method === "PATCH") {
      const patch = yield* decodeJsonBody(ApplicationPatch, request);
      const encoded = yield* Schema.encodeEffect(ApplicationPatch)(patch).pipe(
        Effect.mapError(() => apiError(400, "Application patch could not be encoded")),
      );
      const application = yield* registry
        .update(appId.value, encoded)
        .pipe(
          Effect.mapError((error) =>
            error.message.includes("Application does not exist")
              ? apiError(404, "Application does not exist")
              : apiError(503, "Application registry is unavailable"),
          ),
        );
      if (Option.contains(patch.enabled, false)) {
        yield* terminate(application);
      }
      return yield* json(application);
    }
    if (request.method === "DELETE") {
      const current = Option.fromNullishOr(
        yield* registry.get(appId.value).pipe(registryUnavailable),
      );
      if (Option.isNone(current)) {
        return yield* apiError(404, "Application does not exist");
      }
      const disablePatch = yield* Schema.encodeEffect(ApplicationPatch)({
        enabled: Option.some(false),
        name: Option.none(),
      }).pipe(Effect.mapError(() => apiError(503, "Application patch could not be encoded")));
      const disabled = yield* registry.update(appId.value, disablePatch).pipe(registryUnavailable);
      yield* terminate(disabled);
      const removed = yield* registry.remove(appId.value).pipe(registryUnavailable);
      if (!removed) {
        return yield* apiError(404, "Application does not exist");
      }
      return HttpServerResponse.empty({ headers: RESPONSE_HEADERS, status: 204 });
    }
    return yield* methodNotAllowed("DELETE, GET, PATCH");
  });

  return { handle };
};
