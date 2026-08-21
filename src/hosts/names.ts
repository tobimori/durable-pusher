import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";

export interface WorkerNamesShape {
  readonly worker: string;
}

export class WorkerNames extends Context.Service<WorkerNames, WorkerNamesShape>()(
  "durable-pusher/hosts/WorkerNames",
) {}

const sanitize = (value: string) =>
  value
    .toLowerCase()
    .replaceAll(/[^a-z0-9-]/g, "-")
    .replaceAll(/-+/g, "-")
    .replaceAll(/^-|-$/g, "");

export const WorkerNamesLive = Layer.effect(
  WorkerNames,
  Config.all({
    prefix: Config.string("PUSHER_WORKER_PREFIX").pipe(
      Config.withDefault("durable-pusher"),
    ),
    stage: Config.string("ALCHEMY_STAGE").pipe(Config.withDefault("dev")),
  }).pipe(
    Config.map(({ prefix, stage }) => {
      const base = `${sanitize(prefix)}-${sanitize(stage)}`.slice(0, 48);
      return { worker: base };
    }),
  ),
);
