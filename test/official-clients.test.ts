import { describe, expect, it } from "@effect/vitest";
import PusherServer from "pusher";
import PusherClient from "pusher-js";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const enabled = process.env.PUSHER_E2E === "1";
const host = process.env.PUSHER_E2E_HOST ?? "127.0.0.1";
const port = Number(process.env.PUSHER_E2E_PORT ?? "1337");
const useTLS = process.env.PUSHER_E2E_TLS === "1";
const appId = process.env.PUSHER_APP_ID ?? "local-app";
const appKey = process.env.PUSHER_APP_KEY ?? "local-key";
const appSecret = process.env.PUSHER_APP_SECRET ?? "local-secret";
const encryptionMasterKeyBase64 =
  process.env.PUSHER_ENCRYPTION_MASTER_KEY ??
  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const runId = crypto.randomUUID();
const testChannel = (name: string): string => `${name}-${runId}`;

class OfficialClientError extends Schema.TaggedError<OfficialClientError>()(
  "OfficialClientError",
  {
    cause: Schema.Defect(),
    operation: Schema.String,
  },
) {}

interface Bindable<A> {
  bind(eventName: string, callback: (data: A) => void): void;
  unbind(eventName: string, callback: (data: A) => void): void;
}

interface Triggerable extends Bindable<unknown> {
  trigger(eventName: string, data: unknown): boolean;
}

const waitForEvent = Effect.fn("OfficialClients.waitForEvent")(<A = unknown>(
  bindable: Bindable<A>,
  eventName: string,
) =>
  Effect.callback<A>((resume) => {
    const callback = (data: A) => {
      bindable.unbind(eventName, callback);
      resume(Effect.succeed(data));
    };
    bindable.bind(eventName, callback);
    return Effect.sync(() => bindable.unbind(eventName, callback));
  }).pipe(Effect.timeout("15 seconds")));

const prepareEvent = Effect.fn("OfficialClients.prepareEvent")(
  <A = unknown>(bindable: Bindable<A>, eventName: string) =>
    Effect.sync(() => {
      let completed = Option.none<A>();
      let resumeEvent = Option.none<(effect: Effect.Effect<A>) => void>();
      const callback = (data: A) => {
        bindable.unbind(eventName, callback);
        if (Option.isSome(resumeEvent)) {
          resumeEvent.value(Effect.succeed(data));
        } else {
          completed = Option.some(data);
        }
      };
      bindable.bind(eventName, callback);
      return Effect.callback<A>((resume) => {
        if (Option.isSome(completed)) {
          resume(Effect.succeed(completed.value));
        } else {
          resumeEvent = Option.some(resume);
        }
        return Effect.sync(() => bindable.unbind(eventName, callback));
      }).pipe(Effect.timeout("15 seconds"));
    }),
);

const waitForClientEvent = Effect.fn("OfficialClients.waitForClientEvent")(
  <A>(sender: Triggerable, receiver: Bindable<A>, eventName: string, data: unknown) =>
    Effect.callback<A, OfficialClientError>((resume) => {
      const ready = new Set<string>();
      const trigger = () => {
        if (ready.size !== 2) {
          return;
        }
        queueMicrotask(() => {
          if (!sender.trigger(eventName, data)) {
            resume(
              Effect.fail(
                new OfficialClientError({
                  cause: new Error("Client event trigger was rejected"),
                  operation: "trigger client event",
                }),
              ),
            );
          }
        });
      };
      const senderReady = () => {
        ready.add("sender");
        trigger();
      };
      const receiverReady = () => {
        ready.add("receiver");
        trigger();
      };
      const received = (value: A) => resume(Effect.succeed(value));
      sender.bind("pusher:subscription_succeeded", senderReady);
      receiver.bind("pusher:subscription_succeeded", receiverReady);
      receiver.bind(eventName, received);
      return Effect.sync(() => {
        sender.unbind("pusher:subscription_succeeded", senderReady);
        receiver.unbind("pusher:subscription_succeeded", receiverReady);
        receiver.unbind(eventName, received);
      });
    }).pipe(Effect.timeout("15 seconds")),
);

const makeServer = (encrypted = false) =>
  Effect.sync(
    () =>
      new PusherServer({
        appId,
        host,
        key: appKey,
        port: String(port),
        secret: appSecret,
        useTLS,
        ...(encrypted ? { encryptionMasterKeyBase64 } : {}),
      }),
  );

const makeClient = (
  server: PusherServer,
  presence?: { readonly name: string; readonly userId: string },
) =>
  Effect.acquireRelease(
    Effect.sync(
      () =>
        new PusherClient(appKey, {
          cluster: "mt1",
          disableStats: true,
          enabledTransports: ["ws"],
          forceTLS: useTLS,
          wsHost: host,
          wsPort: port,
          wssPort: port,
          channelAuthorization: {
            customHandler: ({ channelName, socketId }, callback) =>
              callback(
                null,
                server.authorizeChannel(
                  socketId,
                  channelName,
                  presence === undefined
                    ? undefined
                    : {
                        user_id: presence.userId,
                        user_info: { name: presence.name },
                      },
                ),
              ),
          },
        }),
    ),
    (client) => Effect.sync(() => client.disconnect()),
  );

const trigger = Effect.fn("OfficialClients.trigger")(
  (
    server: PusherServer,
    channels: string | Array<string>,
    event: string,
    data: unknown,
  ) =>
    Effect.tryPromise({
      try: () => server.trigger(channels, event, data),
      catch: (cause) => new OfficialClientError({ cause, operation: "trigger" }),
    }),
);

const test = it.effect.skipIf(!enabled);

describe("official Pusher clients", () => {
  test("multiplexes channels and receives signed REST events", () =>
    Effect.gen(function* () {
      const server = yield* makeServer();
      const client = yield* makeClient(server);
      const firstChannel = testChannel("official-room-a");
      const secondChannel = testChannel("official-room-b");
      const first = client.subscribe(firstChannel);
      const firstSubscribed = yield* prepareEvent(first, "pusher:subscription_succeeded");
      const second = client.subscribe(secondChannel);
      const secondSubscribed = yield* prepareEvent(second, "pusher:subscription_succeeded");
      yield* Effect.all([firstSubscribed, secondSubscribed]);
      const firstReceived = yield* prepareEvent<{ readonly message: string }>(
        first,
        "multiplexed-event",
      );
      const secondReceived = yield* prepareEvent<{ readonly message: string }>(
        second,
        "multiplexed-event",
      );
      yield* trigger(server, [firstChannel, secondChannel], "multiplexed-event", {
        message: "roundtrip",
      });
      const received = yield* Effect.all([firstReceived, secondReceived]);
      expect(received).toEqual([{ message: "roundtrip" }, { message: "roundtrip" }]);
      expect(client.connection.socket_id).toMatch(/^\d+\.\d+$/);
    }),
  );

  test("delivers private client events to another connection", () =>
    Effect.gen(function* () {
      const server = yield* makeServer();
      const sender = yield* makeClient(server);
      const receiver = yield* makeClient(server);
      const channel = testChannel("private-official-room");
      const senderChannel = sender.subscribe(channel);
      const receiverChannel = receiver.subscribe(channel);
      const received = yield* waitForClientEvent<{ readonly message: string }>(
        senderChannel,
        receiverChannel,
        "client-official-event",
        { message: "client-roundtrip" },
      );
      expect(received).toEqual({ message: "client-roundtrip" });
    }),
  );

  test("broadcasts subscription count changes", () =>
    Effect.gen(function* () {
      const server = yield* makeServer();
      const first = yield* makeClient(server);
      const second = yield* makeClient(server);
      const channelName = testChannel("subscription-count-room");
      const firstChannel = first.subscribe(channelName);
      const firstCount = yield* prepareEvent<{ readonly subscription_count: number }>(
        firstChannel,
        "pusher:subscription_count",
      );
      yield* waitForEvent(firstChannel, "pusher:subscription_succeeded");
      expect(yield* firstCount).toEqual({ subscription_count: 1 });

      const secondCountForFirst = yield* prepareEvent<{
        readonly subscription_count: number;
      }>(firstChannel, "pusher:subscription_count");
      const secondChannel = second.subscribe(channelName);
      yield* waitForEvent(secondChannel, "pusher:subscription_succeeded");
      expect(yield* secondCountForFirst).toEqual({ subscription_count: 2 });

      const departureCount = yield* prepareEvent<{ readonly subscription_count: number }>(
        firstChannel,
        "pusher:subscription_count",
      );
      second.unsubscribe(channelName);
      expect(yield* departureCount).toEqual({ subscription_count: 1 });
    }),
  );

  test("tracks presence membership", () =>
    Effect.gen(function* () {
      const server = yield* makeServer();
      const first = yield* makeClient(server, { name: "One", userId: "user-one" });
      const channel = testChannel("presence-official-room");
      const firstChannel = first.subscribe(channel);
      yield* waitForEvent(firstChannel, "pusher:subscription_succeeded");
      const memberReceived = yield* prepareEvent<{
        readonly id: string;
        readonly info: { readonly name: string };
      }>(firstChannel, "pusher:member_added");
      const second = yield* makeClient(server, { name: "Two", userId: "user-two" });
      yield* waitForEvent(second.subscribe(channel), "pusher:subscription_succeeded");
      const member = yield* memberReceived;
      expect(member).toEqual({ id: "user-two", info: { name: "Two" } });
      const memberRemoved = yield* prepareEvent<{ readonly id: string }>(
        firstChannel,
        "pusher:member_removed",
      );
      second.disconnect();
      expect((yield* memberRemoved).id).toBe("user-two");
    }),
  );

  test("decrypts encrypted channel events", () =>
    Effect.gen(function* () {
      const server = yield* makeServer(true);
      const client = yield* makeClient(server);
      const channelName = testChannel("private-encrypted-official-room");
      const channel = client.subscribe(channelName);
      yield* waitForEvent(channel, "pusher:subscription_succeeded");
      const encryptedReceived = yield* prepareEvent<{ readonly message: string }>(
        channel,
        "encrypted-event",
      );
      yield* trigger(server, channelName, "encrypted-event", {
        message: "secret-roundtrip",
      });
      const received = yield* encryptedReceived;
      expect(received).toEqual({ message: "secret-roundtrip" });
    }),
  );
});
