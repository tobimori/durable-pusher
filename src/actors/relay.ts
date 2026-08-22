import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { ApplicationPlacement, type ApplicationPlacementEncoded } from "../apps/model.ts";
import { Delivery, mapActorError, type DeliveryEncoded } from "../pusher/protocol.ts";
import { fanoutRelayName, RELAY_FANOUT_WIDTH } from "../sharding.ts";
import { FanoutRelay, type FanoutRelayApi } from "./contracts.ts";
import { FanoutRelayDependencies } from "./dependencies.ts";

export { FanoutRelay };

const partition = <A>(values: ReadonlyArray<A>): ReadonlyArray<ReadonlyArray<A>> => {
  const size = Math.ceil(values.length / RELAY_FANOUT_WIDTH);
  const groups: Array<ReadonlyArray<A>> = [];
  for (let offset = 0; offset < values.length; offset += size) {
    groups.push(values.slice(offset, offset + size));
  }
  return groups;
};

export const FanoutRelayLive = FanoutRelay.make(
  Effect.gen(function* () {
    const { connections, relays } = yield* FanoutRelayDependencies;

    const count = Effect.fn("FanoutRelay.count")(
      function* (
        encodedPlacement: ApplicationPlacementEncoded,
        channel: string,
        gatewayNames: ReadonlyArray<string>,
        path: string,
      ) {
        const placement = yield* Schema.decodeEffect(ApplicationPlacement)(encodedPlacement);
        if (gatewayNames.length <= RELAY_FANOUT_WIDTH) {
          const counts = yield* Effect.forEach(
            gatewayNames,
            Effect.fn("FanoutRelay.count.gateway")(function* (gatewayName) {
              const connection = yield* connections.getByName(gatewayName, placement);
              return yield* connection.count(channel);
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          return counts.reduce((total, value) => total + value, 0);
        }
        const counts = yield* Effect.forEach(
          partition(gatewayNames),
          Effect.fn("FanoutRelay.count.group")(function* (group, index) {
            const childPath = `${path}.${index}`;
            const child = yield* relays.getByName(
              fanoutRelayName(placement.appId, channel, childPath),
              placement,
            );
            return yield* child.count(encodedPlacement, channel, group, childPath);
          }),
          { concurrency: RELAY_FANOUT_WIDTH },
        );
        return counts.reduce((total, value) => total + value, 0);
      },
      Effect.mapError(mapActorError("FanoutRelay", "count")),
    );

    const deliver = Effect.fn("FanoutRelay.deliver")(
      function* (encoded: DeliveryEncoded, gatewayNames: ReadonlyArray<string>, path: string) {
        const delivery = yield* Schema.decodeEffect(Delivery)(encoded);
        const placement: ApplicationPlacement = {
          appId: delivery.appId,
          jurisdiction: delivery.jurisdiction,
          locationHint: delivery.locationHint,
        };
        if (gatewayNames.length <= RELAY_FANOUT_WIDTH) {
          const results = yield* Effect.forEach(
            gatewayNames,
            Effect.fn("FanoutRelay.deliver.gateway")(function* (gatewayName) {
              const result = yield* Effect.gen(function* () {
                const connection = yield* connections.getByName(gatewayName, placement);
                return yield* connection.deliver(encoded);
              }).pipe(Effect.result);
              if (Result.isFailure(result)) {
                yield* Effect.logWarning("Relay gateway delivery failed").pipe(
                  Effect.annotateLogs({ gatewayName, message: result.failure.message }),
                );
              }
              return { gatewayName, result };
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          const stale: string[] = [];
          for (const result of results) {
            if (Result.isSuccess(result.result) && result.result.success === 0) {
              stale.push(result.gatewayName);
            }
          }
          return stale;
        }

        const groups = partition(gatewayNames);
        const results = yield* Effect.forEach(
          groups,
          Effect.fn("FanoutRelay.deliver.group")(function* (group, index) {
            const childPath = `${path}.${index}`;
            const child = yield* relays.getByName(
              fanoutRelayName(delivery.appId, delivery.channel, childPath),
              placement,
            );
            const result = yield* child.deliver(encoded, group, childPath).pipe(Effect.result);
            if (Result.isFailure(result)) {
              yield* Effect.logWarning("Child relay delivery failed").pipe(
                Effect.annotateLogs({ childPath, message: result.failure.message }),
              );
              return [];
            }
            return result.success;
          }),
          { concurrency: RELAY_FANOUT_WIDTH },
        );
        return results.flat();
      },
      Effect.mapError(mapActorError("FanoutRelay", "deliver")),
    );

    const presence = Effect.fn("FanoutRelay.presence")(
      function* (
        encodedPlacement: ApplicationPlacementEncoded,
        channel: string,
        gatewayNames: ReadonlyArray<string>,
        path: string,
      ) {
        const placement = yield* Schema.decodeEffect(ApplicationPlacement)(encodedPlacement);
        if (gatewayNames.length <= RELAY_FANOUT_WIDTH) {
          const members = yield* Effect.forEach(
            gatewayNames,
            Effect.fn("FanoutRelay.presence.gateway")(function* (gatewayName) {
              const connection = yield* connections.getByName(gatewayName, placement);
              return yield* connection.presence(channel);
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          return members.flat();
        }
        const members = yield* Effect.forEach(
          partition(gatewayNames),
          Effect.fn("FanoutRelay.presence.group")(function* (group, index) {
            const childPath = `${path}.${index}`;
            const child = yield* relays.getByName(
              fanoutRelayName(placement.appId, channel, childPath),
              placement,
            );
            return yield* child.presence(encodedPlacement, channel, group, childPath);
          }),
          { concurrency: RELAY_FANOUT_WIDTH },
        );
        return members.flat();
      },
      Effect.mapError(mapActorError("FanoutRelay", "presence")),
    );

    const terminateApplication = Effect.fn("FanoutRelay.terminateApplication")(
      function* (
        encodedPlacement: ApplicationPlacementEncoded,
        generation: number,
        gatewayNames: ReadonlyArray<string>,
        path: string,
      ) {
        const placement = yield* Schema.decodeEffect(ApplicationPlacement)(encodedPlacement);
        if (gatewayNames.length <= RELAY_FANOUT_WIDTH) {
          const totals = yield* Effect.forEach(
            gatewayNames,
            Effect.fn("FanoutRelay.terminateApplication.gateway")(function* (gatewayName) {
              return yield* Effect.gen(function* () {
                const connection = yield* connections.getByName(gatewayName, placement);
                return yield* connection.terminateApplication(placement.appId, generation);
              }).pipe(Effect.result);
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          let total = 0;
          for (const result of totals) {
            if (Result.isFailure(result)) {
              return yield* result.failure;
            }
            total += result.success;
          }
          return total;
        }
        const totals = yield* Effect.forEach(
          partition(gatewayNames),
          Effect.fn("FanoutRelay.terminateApplication.group")(function* (group, index) {
            const childPath = `${path}.${index}`;
            return yield* Effect.gen(function* () {
              const child = yield* relays.getByName(
                fanoutRelayName(placement.appId, "application-control", childPath),
                placement,
              );
              return yield* child.terminateApplication(
                encodedPlacement,
                generation,
                group,
                childPath,
              );
            }).pipe(Effect.result);
          }),
          { concurrency: RELAY_FANOUT_WIDTH },
        );
        let total = 0;
        for (const result of totals) {
          if (Result.isFailure(result)) {
            return yield* result.failure;
          }
          total += result.success;
        }
        return total;
      },
      Effect.mapError(mapActorError("FanoutRelay", "terminateApplication")),
    );

    const terminateUser = Effect.fn("FanoutRelay.terminateUser")(
      function* (
        encodedPlacement: ApplicationPlacementEncoded,
        userId: string,
        gatewayNames: ReadonlyArray<string>,
        path: string,
      ) {
        const placement = yield* Schema.decodeEffect(ApplicationPlacement)(encodedPlacement);
        if (gatewayNames.length <= RELAY_FANOUT_WIDTH) {
          const totals = yield* Effect.forEach(
            gatewayNames,
            Effect.fn("FanoutRelay.terminateUser.gateway")(function* (gatewayName) {
              return yield* Effect.gen(function* () {
                const connection = yield* connections.getByName(gatewayName, placement);
                return yield* connection.terminateUser(placement.appId, userId);
              }).pipe(Effect.result);
            }),
            { concurrency: RELAY_FANOUT_WIDTH },
          );
          let total = 0;
          for (const result of totals) {
            if (Result.isFailure(result)) {
              return yield* result.failure;
            }
            total += result.success;
          }
          return total;
        }
        const totals = yield* Effect.forEach(
          partition(gatewayNames),
          Effect.fn("FanoutRelay.terminateUser.group")(function* (group, index) {
            const childPath = `${path}.${index}`;
            return yield* Effect.gen(function* () {
              const child = yield* relays.getByName(
                fanoutRelayName(placement.appId, `#server-to-user-${userId}`, childPath),
                placement,
              );
              return yield* child.terminateUser(encodedPlacement, userId, group, childPath);
            }).pipe(Effect.result);
          }),
          { concurrency: RELAY_FANOUT_WIDTH },
        );
        let total = 0;
        for (const result of totals) {
          if (Result.isFailure(result)) {
            return yield* result.failure;
          }
          total += result.success;
        }
        return total;
      },
      Effect.mapError(mapActorError("FanoutRelay", "terminateUser")),
    );

    const api = {
      count,
      deliver,
      presence,
      terminateApplication,
      terminateUser,
    } satisfies FanoutRelayApi;
    // Alchemy's Durable Object constructor is intentionally a two-phase Effect.
    // @effect-diagnostics-next-line returnEffectInGen:off
    return Effect.succeed(api);
  }),
);
