import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ApplicationSummary, ProvisionedApplication } from "../src/apps/model.ts";

const enabled = process.env.PUSHER_E2E === "1";
const host = process.env.PUSHER_E2E_HOST ?? "127.0.0.1";
const port = Number(process.env.PUSHER_E2E_PORT ?? "1337");
const useTLS = process.env.PUSHER_E2E_TLS === "1";
const controlToken = process.env.PUSHER_CONTROL_TOKEN ?? "control-token";
const origin = `${useTLS ? "https" : "http"}://${host}:${port}`;

class ControlClientError extends Schema.TaggedError<ControlClientError>()("ControlClientError", {
  cause: Schema.Defect(),
  operation: Schema.String,
}) {}

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
const decodeProvisionedApplication = Schema.decodeEffect(
  Schema.fromJsonString(ProvisionedApplication),
  { onExcessProperty: "error" },
);
const decodeApplicationSummary = Schema.decodeEffect(Schema.fromJsonString(ApplicationSummary), {
  onExcessProperty: "error",
});
const decodeApplicationList = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Struct({ apps: Schema.Array(ApplicationSummary) })),
  { onExcessProperty: "error" },
);

const request = Effect.fn("ControlApi.request")(function* (
  method: string,
  path: string,
  body: Option.Option<string> = Option.none(),
  authorization: Option.Option<string> = Option.some(controlToken),
) {
  const response = yield* Effect.tryPromise({
    try: () =>
      fetch(`${origin}${path}`, {
        method,
        headers: {
          ...(Option.isSome(authorization)
            ? { authorization: `Bearer ${authorization.value}` }
            : {}),
          ...(Option.isSome(body) ? { "content-type": "application/json" } : {}),
        },
        ...(Option.isSome(body) ? { body: body.value } : {}),
      }),
    catch: (cause) => new ControlClientError({ cause, operation: `${method} ${path}` }),
  });
  const text = yield* Effect.tryPromise({
    try: () => response.text(),
    catch: (cause) => new ControlClientError({ cause, operation: "read response" }),
  });
  return {
    allow: Option.fromNullishOr(response.headers.get("allow")),
    allowMethods: Option.fromNullishOr(response.headers.get("access-control-allow-methods")),
    status: response.status,
    text,
  };
});

const test = it.effect.skipIf(!enabled);

describe("application control API", () => {
  test("authenticates and manages an application lifecycle", () =>
    Effect.gen(function* () {
      const appId = `control-${crypto.randomUUID()}`;
      const path = `/control/v1/apps/${appId}`;

      const unauthorized = yield* request("GET", "/control/v1/apps", Option.none(), Option.none());
      expect(unauthorized.status).toBe(403);
      const wrongToken = yield* request(
        "GET",
        "/control/v1/apps",
        Option.none(),
        Option.some("wrong-token"),
      );
      expect(wrongToken.status).toBe(403);

      const preflight = yield* request("OPTIONS", path, Option.none(), Option.none());
      expect(preflight.status).toBe(204);
      expect(Option.getOrElse(preflight.allowMethods, () => "").includes("DELETE")).toBe(true);
      expect(Option.getOrElse(preflight.allowMethods, () => "").includes("PATCH")).toBe(true);

      const createBody = yield* encodeJson({
        appId,
        jurisdiction: "us",
        locationHint: "wnam",
        name: "Control integration",
      });
      const createdResponse = yield* request("POST", "/control/v1/apps", Option.some(createBody));
      expect(createdResponse.status).toBe(201);
      const created = yield* decodeProvisionedApplication(createdResponse.text);
      expect(created.appId).toBe(appId);
      expect(created.authToken.length).toBeGreaterThan(0);
      expect(Option.getOrElse(created.jurisdiction, () => "none")).toBe("us");
      expect(Option.getOrElse(created.locationHint, () => "none")).toBe("wnam");

      const listResponse = yield* request("GET", "/control/v1/apps");
      expect(listResponse.status).toBe(200);
      const list = yield* decodeApplicationList(listResponse.text);
      expect(list.apps.some((application) => application.appId === appId)).toBe(true);

      const getResponse = yield* request("GET", path);
      expect(getResponse.status).toBe(200);
      const summary = yield* decodeApplicationSummary(getResponse.text);
      expect(summary.appKey).toBe(created.appKey);

      const invalidPatch = yield* request("PATCH", path, Option.some('{"jurisdiction":"eu"}'));
      expect(invalidPatch.status).toBe(400);
      const emptyPatch = yield* request("PATCH", path, Option.some("{}"));
      expect(emptyPatch.status).toBe(400);
      const unsupportedMethod = yield* request("PUT", path);
      expect(unsupportedMethod.status).toBe(405);
      expect(Option.getOrElse(unsupportedMethod.allow, () => "").includes("PATCH")).toBe(true);

      const patchResponse = yield* request(
        "PATCH",
        path,
        Option.some('{"enabled":false,"name":"Disabled integration"}'),
      );
      expect(patchResponse.status).toBe(200);
      const patched = yield* decodeApplicationSummary(patchResponse.text);
      expect(patched.name).toBe("Disabled integration");
      expect(patched.status).toBe("disabled");

      const deleteResponse = yield* request("DELETE", path);
      expect(deleteResponse.status).toBe(204);

      const missingResponse = yield* request("GET", path);
      expect(missingResponse.status).toBe(404);

      const reusedResponse = yield* request("POST", "/control/v1/apps", Option.some(createBody));
      expect(reusedResponse.status).toBe(409);
    }));
});
