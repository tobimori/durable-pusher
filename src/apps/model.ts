import * as Schema from "effect/Schema";

export const APP_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
export const APP_KEY_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;

export const AppId = Schema.String.check(Schema.isPattern(APP_ID_PATTERN));
export const AppKey = Schema.String.check(Schema.isPattern(APP_KEY_PATTERN));
export const AppStatus = Schema.Literals(["active", "disabled", "deleted"]);
export const DurableObjectJurisdiction = Schema.Literals([
  "eu",
  "fedramp",
  "fedramp-high",
  "us",
]);
export const DurableObjectLocationHint = Schema.Literals([
  "afr",
  "apac",
  "apac-ne",
  "apac-se",
  "eeur",
  "enam",
  "me",
  "oc",
  "sam",
  "weur",
  "wnam",
]);

export const ApplicationCreate = Schema.Struct({
  appId: Schema.optionalKey(AppId),
  jurisdiction: Schema.optionalKey(Schema.NullOr(DurableObjectJurisdiction)),
  locationHint: Schema.optionalKey(Schema.NullOr(DurableObjectLocationHint)),
  name: Schema.String.check(Schema.isLengthBetween(1, 100)),
});

export const ApplicationPatch = Schema.Struct({
  enabled: Schema.optionalKey(Schema.Boolean),
  jurisdiction: Schema.optionalKey(Schema.NullOr(DurableObjectJurisdiction)),
  locationHint: Schema.optionalKey(Schema.NullOr(DurableObjectLocationHint)),
  name: Schema.optionalKey(Schema.String.check(Schema.isLengthBetween(1, 100))),
});

export const ApplicationBootstrap = Schema.Struct({
  appId: AppId,
  appKey: AppKey,
  appSecret: Schema.String,
  authToken: Schema.String,
  encryptionMasterKey: Schema.String,
  jurisdiction: Schema.optionalKey(Schema.NullOr(DurableObjectJurisdiction)),
  locationHint: Schema.optionalKey(Schema.NullOr(DurableObjectLocationHint)),
  name: Schema.String.check(Schema.isLengthBetween(1, 100)),
});

export const ApplicationSummary = Schema.Struct({
  appId: AppId,
  appKey: AppKey,
  createdAt: Schema.Int,
  jurisdiction: Schema.NullOr(DurableObjectJurisdiction),
  locationHint: Schema.NullOr(DurableObjectLocationHint),
  name: Schema.String,
  status: AppStatus,
  updatedAt: Schema.Int,
});

export const RuntimeApplication = Schema.Struct({
  ...ApplicationSummary.fields,
  appSecret: Schema.String,
  encryptionMasterKey: Schema.String,
});

export const ProvisionedApplication = Schema.Struct({
  ...RuntimeApplication.fields,
  authToken: Schema.String,
});

export type ApplicationCreate = typeof ApplicationCreate.Type;
export type ApplicationPatch = typeof ApplicationPatch.Type;
export type ApplicationBootstrap = typeof ApplicationBootstrap.Type;
export type ApplicationSummary = typeof ApplicationSummary.Type;
export type RuntimeApplication = typeof RuntimeApplication.Type;
export type ProvisionedApplication = typeof ProvisionedApplication.Type;
export type DurableObjectJurisdiction = typeof DurableObjectJurisdiction.Type;
export type DurableObjectLocationHint = typeof DurableObjectLocationHint.Type;
