import { describe, expect, it } from "@effect/vitest";
import PusherServer from "pusher";
import PusherClient from "pusher-js";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";

const enabled = process.env.PUSHER_E2E === "1";
const host = process.env.PUSHER_E2E_HOST ?? "127.0.0.1";
const port = Number(process.env.PUSHER_E2E_PORT ?? "1338");
const useTLS = process.env.PUSHER_E2E_TLS === "1";
const appId = process.env.PUSHER_APP_ID ?? "local-app";
const appKey = process.env.PUSHER_APP_KEY ?? "local-key";
const appSecret = process.env.PUSHER_APP_SECRET ?? "local-secret";
const encryptionMasterKeyBase64 =
  process.env.PUSHER_ENCRYPTION_MASTER_KEY ??
  "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

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
      const first = client.subscribe("official-room-a");
      const second = client.subscribe("official-room-b");
      yield* Effect.all([
        waitForEvent(first, "pusher:subscription_succeeded"),
        waitForEvent(second, "pusher:subscription_succeeded"),
      ]);
      const received = yield* Effect.all([
        waitForEvent<{ readonly message: string }>(first, "multiplexed-event"),
        waitForEvent<{ readonly message: string }>(second, "multiplexed-event"),
      ]).pipe(
        Effect.forkChild,
        Effect.tap(() =>
          trigger(server, ["official-room-a", "official-room-b"], "multiplexed-event", {
            message: "roundtrip",
          }),
        ),
        Effect.flatMap(Fiber.join),
      );
      expect(received).toEqual([{ message: "roundtrip" }, { message: "roundtrip" }]);
      expect(client.connection.socket_id).toMatch(/^\d+\.\d+$/);
    }),
  );

  test("delivers private client events to another connection", () =>
    Effect.gen(function* () {
      const server = yield* makeServer();
      const sender = yield* makeClient(server);
      const receiver = yield* makeClient(server);
      const senderChannel = sender.subscribe("private-official-room");
      const receiverChannel = receiver.subscribe("private-official-room");
      yield* Effect.all([
        waitForEvent(senderChannel, "pusher:subscription_succeeded"),
        waitForEvent(receiverChannel, "pusher:subscription_succeeded"),
      ]);
      const received = yield* waitForEvent<{ readonly message: string }>(
        receiverChannel,
        "client-official-event",
      ).pipe(
        Effect.forkChild,
        Effect.tap(() =>
          Effect.sync(() =>
            senderChannel.trigger("client-official-event", { message: "client-roundtrip" }),
          ),
        ),
        Effect.flatMap(Fiber.join),
      );
      expect(received).toEqual({ message: "client-roundtrip" });
    }),
  );

  test("tracks presence membership", () =>
    Effect.gen(function* () {
      const server = yield* makeServer();
      const first = yield* makeClient(server, { name: "One", userId: "user-one" });
      const firstChannel = first.subscribe("presence-official-room");
      yield* waitForEvent(firstChannel, "pusher:subscription_succeeded");
      const member = yield* waitForEvent<{
        readonly id: string;
        readonly info: { readonly name: string };
      }>(firstChannel, "pusher:member_added").pipe(
        Effect.forkChild,
        Effect.tap(() =>
          Effect.gen(function* () {
            const second = yield* makeClient(server, { name: "Two", userId: "user-two" });
            yield* waitForEvent(
              second.subscribe("presence-official-room"),
              "pusher:subscription_succeeded",
            );
          }),
        ),
        Effect.flatMap(Fiber.join),
      );
      expect(member).toEqual({ id: "user-two", info: { name: "Two" } });
    }),
  );

  test("decrypts encrypted channel events", () =>
    Effect.gen(function* () {
      const server = yield* makeServer(true);
      const client = yield* makeClient(server);
      const channel = client.subscribe("private-encrypted-official-room");
      yield* waitForEvent(channel, "pusher:subscription_succeeded");
      const received = yield* waitForEvent<{ readonly message: string }>(
        channel,
        "encrypted-event",
      ).pipe(
        Effect.forkChild,
        Effect.tap(() =>
          trigger(server, "private-encrypted-official-room", "encrypted-event", {
            message: "secret-roundtrip",
          }),
        ),
        Effect.flatMap(Fiber.join),
      );
      expect(received).toEqual({ message: "secret-roundtrip" });
    }),
  );
});
