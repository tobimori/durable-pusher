import * as Schema from "effect/Schema";

export const APP_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
export const APP_KEY_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;

export const AppId = Schema.String.check(Schema.isPattern(APP_ID_PATTERN));
export const AppKey = Schema.String.check(Schema.isPattern(APP_KEY_PATTERN));
export const AppStatus = Schema.Literals(["active", "disabled", "deleted"]);
export const DurableObjectJurisdiction = Schema.Literals(["eu", "fedramp", "fedramp-high", "us"]);
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
  appId: Schema.OptionFromOptionalKey(AppId),
  jurisdiction: Schema.OptionFromOptionalNullOr(DurableObjectJurisdiction),
  locationHint: Schema.OptionFromOptionalNullOr(DurableObjectLocationHint),
  name: Schema.String.check(Schema.isLengthBetween(1, 100)),
});

export const ApplicationPatch = Schema.Struct({
  enabled: Schema.OptionFromOptionalKey(Schema.Boolean),
  name: Schema.OptionFromOptionalKey(Schema.String.check(Schema.isLengthBetween(1, 100))),
});

export const ApplicationBootstrap = Schema.Struct({
  appId: AppId,
  appKey: AppKey,
  appSecret: Schema.String,
  authToken: Schema.String,
  encryptionMasterKey: Schema.String,
  jurisdiction: Schema.OptionFromOptionalNullOr(DurableObjectJurisdiction),
  locationHint: Schema.OptionFromOptionalNullOr(DurableObjectLocationHint),
  name: Schema.String.check(Schema.isLengthBetween(1, 100)),
});

export const ApplicationSummary = Schema.Struct({
  appId: AppId,
  appKey: AppKey,
  createdAt: Schema.Int,
  jurisdiction: Schema.OptionFromNullOr(DurableObjectJurisdiction),
  locationHint: Schema.OptionFromNullOr(DurableObjectLocationHint),
  name: Schema.String,
  status: AppStatus,
  updatedAt: Schema.Int,
});

export const ApplicationPlacement = Schema.Struct({
  appId: AppId,
  jurisdiction: Schema.OptionFromNullOr(DurableObjectJurisdiction),
  locationHint: Schema.OptionFromNullOr(DurableObjectLocationHint),
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
export type ApplicationCreateEncoded = typeof ApplicationCreate.Encoded;
export type ApplicationPatch = typeof ApplicationPatch.Type;
export type ApplicationPatchEncoded = typeof ApplicationPatch.Encoded;
export type ApplicationBootstrap = typeof ApplicationBootstrap.Type;
export type ApplicationBootstrapEncoded = typeof ApplicationBootstrap.Encoded;
export type ApplicationSummary = typeof ApplicationSummary.Type;
export type ApplicationSummaryEncoded = typeof ApplicationSummary.Encoded;
export type ApplicationPlacement = typeof ApplicationPlacement.Type;
export type ApplicationPlacementEncoded = typeof ApplicationPlacement.Encoded;
export type RuntimeApplication = typeof RuntimeApplication.Type;
export type RuntimeApplicationEncoded = typeof RuntimeApplication.Encoded;
export type ProvisionedApplication = typeof ProvisionedApplication.Type;
export type ProvisionedApplicationEncoded = typeof ProvisionedApplication.Encoded;
export type DurableObjectJurisdiction = typeof DurableObjectJurisdiction.Type;
export type DurableObjectLocationHint = typeof DurableObjectLocationHint.Type;
