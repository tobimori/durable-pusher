import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";

export interface AppConfigShape {
  readonly appId: string;
  readonly appKey: string;
  readonly appSecret: Redacted.Redacted<string>;
  readonly authToken: Redacted.Redacted<string>;
  readonly controlToken: Redacted.Redacted<string>;
  readonly encryptionMasterKey: Redacted.Redacted<string>;
}

export class AppConfig extends Context.Service<AppConfig, AppConfigShape>()("AppConfig") {}

export const AppConfigLive = Layer.effect(
  AppConfig,
  Config.all({
    appId: Config.string("PUSHER_APP_ID"),
    appKey: Config.string("PUSHER_APP_KEY"),
    appSecret: Config.redacted("PUSHER_APP_SECRET"),
    authToken: Config.redacted("PUSHER_AUTH_TOKEN"),
    controlToken: Config.redacted("PUSHER_CONTROL_TOKEN"),
    encryptionMasterKey: Config.redacted("PUSHER_ENCRYPTION_MASTER_KEY").pipe(
      Config.withDefault(Redacted.make("")),
    ),
  }),
);
