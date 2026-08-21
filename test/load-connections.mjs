import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import PusherServer from "pusher";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

class LoadTestError extends Schema.TaggedError()("LoadTestError", {
  connection: Schema.Int,
  message: Schema.String,
  operation: Schema.String,
}) {}

const ServerFrame = Schema.Struct({
  channel: Schema.optionalKey(Schema.String),
  data: Schema.optionalKey(Schema.Json),
  event: Schema.String,
});
const EstablishedData = Schema.Struct({ socket_id: Schema.String });
const ClientEventData = Schema.Struct({ sender: Schema.String });
const decodeServerFrame = Schema.decodeUnknownResult(Schema.fromJsonString(ServerFrame));
const decodeEstablishedData = Schema.decodeUnknownResult(Schema.fromJsonString(EstablishedData));
const decodeClientEventData = Schema.decodeUnknownResult(Schema.fromJsonString(ClientEventData));
const ClientFrame = Schema.Struct({
  channel: Schema.optionalKey(Schema.String),
  data: Schema.Json,
  event: Schema.String,
});
const encodeClientFrame = Schema.encodeEffect(Schema.fromJsonString(ClientFrame));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const PresenceRoomSize = PositiveInt.check(Schema.isLessThanOrEqualTo(100));
const clientEventName = "client-presence-load";

const config = Config.all({
  appId: Config.string("PUSHER_APP_ID").pipe(Config.withDefault("local-app")),
  appKey: Config.string("PUSHER_APP_KEY").pipe(Config.withDefault("local-key")),
  appSecret: Config.string("PUSHER_APP_SECRET").pipe(Config.withDefault("local-secret")),
  concurrency: Config.int("PUSHER_LOAD_CONCURRENCY").pipe(Config.withDefault(100)),
  connectTimeoutMs: Config.int("PUSHER_LOAD_CONNECT_TIMEOUT_MS").pipe(
    Config.withDefault(30_000),
  ),
  connections: Config.int("PUSHER_LOAD_CONNECTIONS").pipe(Config.withDefault(1_000)),
  eventTimeoutMs: Config.int("PUSHER_LOAD_EVENT_TIMEOUT_MS").pipe(
    Config.withDefault(300_000),
  ),
  heartbeatSeconds: Config.int("PUSHER_LOAD_HEARTBEAT_SECONDS").pipe(Config.withDefault(60)),
  host: Config.string("PUSHER_E2E_HOST").pipe(Config.withDefault("127.0.0.1")),
  idleSeconds: Config.int("PUSHER_LOAD_IDLE_SECONDS").pipe(Config.withDefault(5)),
  port: Config.int("PUSHER_E2E_PORT").pipe(Config.withDefault(1337)),
  roomSize: Config.int("PUSHER_LOAD_PRESENCE_ROOM_SIZE").pipe(Config.withDefault(100)),
  tls: Config.string("PUSHER_E2E_TLS").pipe(
    Config.withDefault("0"),
    Config.map((value) => value === "1"),
  ),
});

const loadError = (connection, operation, message) =>
  new LoadTestError({ connection, message, operation });

const positiveInt = Effect.fn("ConnectionLoad.positiveInt")((name, value) =>
  Schema.decodeEffect(PositiveInt)(value).pipe(
    Effect.mapError(() => loadError(0, "configuration", `${name} must be a positive integer`)),
  ),
);

const presenceRoomSize = Effect.fn("ConnectionLoad.presenceRoomSize")((value) =>
  Schema.decodeEffect(PresenceRoomSize)(value).pipe(
    Effect.mapError(() =>
      loadError(
        0,
        "configuration",
        "PUSHER_LOAD_PRESENCE_ROOM_SIZE must be between 1 and 100",
      ),
    ),
  ),
);

const makeUrl = (settings) => {
  const scheme = settings.tls ? "wss" : "ws";
  const url = new URL(
    `${scheme}://${settings.host}:${settings.port}/app/${encodeURIComponent(settings.appKey)}`,
  );
  url.searchParams.set("client", "durable-pusher-load-test");
  url.searchParams.set("flash", "false");
  url.searchParams.set("protocol", "7");
  url.searchParams.set("version", "1.0");
  return url;
};

const waitForEstablished = Effect.fn("ConnectionLoad.waitForEstablished")(
  (socket, connection, timeoutMs) =>
    Effect.callback((resume) => {
      let settled = false;
      const finish = (effect) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        socket.removeEventListener("close", onClose);
        socket.removeEventListener("error", onError);
        socket.removeEventListener("message", onMessage);
        resume(effect);
      };
      const onClose = (event) =>
        finish(
          Effect.fail(
            loadError(
              connection,
              "connect",
              `socket closed before establishment (${event.code}: ${event.reason})`,
            ),
          ),
        );
      const onError = () =>
        finish(Effect.fail(loadError(connection, "connect", "WebSocket connection failed")));
      const onMessage = (event) => {
        const decoded = decodeServerFrame(event.data);
        if (Result.isFailure(decoded)) {
          return;
        }
        if (decoded.success.event === "pusher:error") {
          finish(Effect.fail(loadError(connection, "connect", "server rejected the connection")));
          return;
        }
        if (
          decoded.success.event !== "pusher:connection_established" ||
          typeof decoded.success.data !== "string"
        ) {
          return;
        }
        const established = decodeEstablishedData(decoded.success.data);
        finish(
          Result.isSuccess(established)
            ? Effect.succeed(established.success.socket_id)
            : Effect.fail(
                loadError(connection, "connect", "server returned invalid connection metadata"),
              ),
        );
      };
      const timeout = setTimeout(
        () =>
          finish(
            Effect.fail(
              loadError(connection, "connect", `timed out after ${timeoutMs} milliseconds`),
            ),
          ),
        timeoutMs,
      );
      socket.addEventListener("close", onClose);
      socket.addEventListener("error", onError);
      socket.addEventListener("message", onMessage);
      return Effect.sync(() => {
        clearTimeout(timeout);
        socket.removeEventListener("close", onClose);
        socket.removeEventListener("error", onError);
        socket.removeEventListener("message", onMessage);
      });
    }),
);

const openConnection = Effect.fn("ConnectionLoad.openConnection")(function* (
  url,
  connection,
  timeoutMs,
) {
  const socket = yield* Effect.try({
    try: () => new WebSocket(url),
    catch: (cause) => loadError(connection, "connect", String(cause)),
  });
  const socketId = yield* waitForEstablished(socket, connection, timeoutMs).pipe(
    Effect.onError(() =>
      Effect.sync(() => {
        socket.close();
      }),
    ),
  );
  return { connection, socket, socketId };
});

const closeConnections = Effect.fn("ConnectionLoad.closeConnections")((connections) =>
  Effect.sync(() => {
    for (const { socket } of connections) {
      if (socket.readyState === WebSocket.OPEN) {
        socket.close(1000, "load test complete");
      }
    }
  }),
);

const keepConnectionsAlive = Effect.fn("ConnectionLoad.keepConnectionsAlive")(
  (connections, pingFrame, heartbeatSeconds) => {
    const batchSize = 50;
    const batches = Array.from(
      { length: Math.ceil(connections.length / batchSize) },
      (_, index) => connections.slice(index * batchSize, (index + 1) * batchSize),
    );
    const delayMs = Math.max(100, Math.floor((heartbeatSeconds * 1_000) / batches.length));
    return Effect.forEach(
      batches,
      (batch) =>
        Effect.sync(() => {
          for (const { socket } of batch) {
            if (socket.readyState === WebSocket.OPEN) {
              socket.send(pingFrame);
            }
          }
        }).pipe(Effect.andThen(Effect.sleep(Duration.millis(delayMs)))),
      { discard: true },
    );
  },
);

const subscribePresence = Effect.fn("ConnectionLoad.subscribePresence")(
  (session, frame, timeoutMs) =>
    Effect.callback((resume) => {
      let settled = false;
      const finish = (effect) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        session.socket.removeEventListener("close", onClose);
        session.socket.removeEventListener("error", onError);
        session.socket.removeEventListener("message", onMessage);
        resume(effect);
      };
      const onClose = (event) =>
        finish(
          Effect.fail(
            loadError(
              session.connection,
              "subscribe",
              `socket closed before subscription (${event.code}: ${event.reason})`,
            ),
          ),
        );
      const onError = () =>
        finish(
          Effect.fail(loadError(session.connection, "subscribe", "presence subscription failed")),
        );
      const onMessage = (event) => {
        const decoded = decodeServerFrame(event.data);
        if (Result.isFailure(decoded)) {
          return;
        }
        if (decoded.success.event === "pusher:error") {
          finish(
            Effect.fail(
              loadError(session.connection, "subscribe", "server rejected presence subscription"),
            ),
          );
        } else if (
          decoded.success.event === "pusher_internal:subscription_succeeded" &&
          decoded.success.channel === session.channel
        ) {
          finish(Effect.void);
        }
      };
      const timeout = setTimeout(
        () =>
          finish(
            Effect.fail(
              loadError(session.connection, "subscribe", `timed out after ${timeoutMs} milliseconds`),
            ),
          ),
        timeoutMs,
      );
      session.socket.addEventListener("close", onClose);
      session.socket.addEventListener("error", onError);
      session.socket.addEventListener("message", onMessage);
      session.socket.send(frame);
      return Effect.sync(() => {
        clearTimeout(timeout);
        session.socket.removeEventListener("close", onClose);
        session.socket.removeEventListener("error", onError);
        session.socket.removeEventListener("message", onMessage);
      });
    }),
);

const joinPresence = Effect.fn("ConnectionLoad.joinPresence")(
  function* (server, session, timeoutMs) {
    const authorization = yield* Effect.try({
      try: () =>
        server.authorizeChannel(session.socketId, session.channel, {
          user_id: session.userId,
          user_info: { connection: session.connection },
        }),
      catch: (cause) => loadError(session.connection, "authorize", String(cause)),
    });
    const frame = yield* encodeClientFrame({
      data: {
        auth: authorization.auth,
        channel: session.channel,
        channel_data: authorization.channel_data,
      },
      event: "pusher:subscribe",
    }).pipe(
      Effect.mapError(() =>
        loadError(session.connection, "subscribe", "could not encode subscription frame"),
      ),
    );
    yield* subscribePresence(session, frame, timeoutMs);
  },
);

const exchangeClientEvents = Effect.fn("ConnectionLoad.exchangeClientEvents")(
  (sessions, roomMembers, timeoutMs) =>
    Effect.callback((resume) => {
      let settled = false;
      let completed = 0;
      const receivedByConnection = new Map();
      const listeners = [];
      const cleanup = () => {
        clearTimeout(timeout);
        for (const { onClose, onError, onMessage, session } of listeners) {
          session.socket.removeEventListener("close", onClose);
          session.socket.removeEventListener("error", onError);
          session.socket.removeEventListener("message", onMessage);
        }
      };
      const finish = (effect) => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        resume(effect);
      };

      for (const session of sessions) {
        const received = new Set();
        receivedByConnection.set(session.connection, received);
        const expected = roomMembers.get(session.channel)?.size ?? 0;
        if (expected === 1) {
          completed += 1;
        }
        const onClose = (event) =>
          finish(
            Effect.fail(
              loadError(
                session.connection,
                "client event",
                `socket closed during exchange (${event.code}: ${event.reason})`,
              ),
            ),
          );
        const onError = () =>
          finish(
            Effect.fail(
              loadError(session.connection, "client event", "client event exchange failed"),
            ),
          );
        const onMessage = (event) => {
          const decoded = decodeServerFrame(event.data);
          if (Result.isFailure(decoded)) {
            return;
          }
          if (decoded.success.event === "pusher:error") {
            finish(
              Effect.fail(
                loadError(session.connection, "client event", "server rejected a client event"),
              ),
            );
            return;
          }
          if (
            decoded.success.event !== clientEventName ||
            decoded.success.channel !== session.channel ||
            typeof decoded.success.data !== "string"
          ) {
            return;
          }
          const eventData = decodeClientEventData(decoded.success.data);
          if (Result.isFailure(eventData)) {
            finish(
              Effect.fail(
                loadError(session.connection, "client event", "received malformed client event"),
              ),
            );
            return;
          }
          const members = roomMembers.get(session.channel);
          if (
            members === undefined ||
            !members.has(eventData.success.sender) ||
            eventData.success.sender === session.userId
          ) {
            finish(
              Effect.fail(
                loadError(session.connection, "client event", "received event from invalid sender"),
              ),
            );
            return;
          }
          const before = received.size;
          received.add(eventData.success.sender);
          if (received.size !== before && received.size === members.size - 1) {
            completed += 1;
            if (completed === sessions.length) {
              finish(Effect.void);
            }
          }
        };
        listeners.push({ onClose, onError, onMessage, session });
        session.socket.addEventListener("close", onClose);
        session.socket.addEventListener("error", onError);
        session.socket.addEventListener("message", onMessage);
      }

      const timeout = setTimeout(
        () => {
          const incomplete = sessions.find((session) => {
            const received = receivedByConnection.get(session.connection)?.size ?? 0;
            const expected = (roomMembers.get(session.channel)?.size ?? 1) - 1;
            return received !== expected;
          });
          const received =
            incomplete === undefined
              ? 0
              : (receivedByConnection.get(incomplete.connection)?.size ?? 0);
          const expected =
            incomplete === undefined ? 0 : (roomMembers.get(incomplete.channel)?.size ?? 1) - 1;
          finish(
            Effect.fail(
              loadError(
                incomplete?.connection ?? 0,
                "client event",
                `timed out after ${timeoutMs} milliseconds (${received}/${expected} peer events received)`,
              ),
            ),
          );
        },
        timeoutMs,
      );

      if (completed === sessions.length) {
        finish(Effect.void);
        return Effect.sync(cleanup);
      }

      for (const session of sessions) {
        session.socket.send(session.clientEventFrame);
      }
      return Effect.sync(cleanup);
    }),
);

const program = Effect.scoped(
  Effect.gen(function* () {
    const settings = yield* config;
    const connectionCount = yield* positiveInt("PUSHER_LOAD_CONNECTIONS", settings.connections);
    const concurrency = yield* positiveInt("PUSHER_LOAD_CONCURRENCY", settings.concurrency);
    const connectTimeoutMs = yield* positiveInt(
      "PUSHER_LOAD_CONNECT_TIMEOUT_MS",
      settings.connectTimeoutMs,
    );
    const eventTimeoutMs = yield* positiveInt(
      "PUSHER_LOAD_EVENT_TIMEOUT_MS",
      settings.eventTimeoutMs,
    );
    const heartbeatSeconds = yield* positiveInt(
      "PUSHER_LOAD_HEARTBEAT_SECONDS",
      settings.heartbeatSeconds,
    );
    const idleSeconds = yield* positiveInt("PUSHER_LOAD_IDLE_SECONDS", settings.idleSeconds);
    const roomSize = yield* presenceRoomSize(settings.roomSize);
    const url = makeUrl(settings);
    const server = new PusherServer({
      appId: settings.appId,
      host: settings.host,
      key: settings.appKey,
      port: String(settings.port),
      secret: settings.appSecret,
      useTLS: settings.tls,
    });
    const runId = crypto.randomUUID();
    const connectionIndexes = Array.from({ length: connectionCount }, (_, index) => index + 1);
    const openedConnections = yield* Ref.make([]);
    yield* Effect.addFinalizer(() =>
      Ref.get(openedConnections).pipe(Effect.flatMap(closeConnections)),
    );
    const startedAt = yield* Clock.currentTimeMillis;

    yield* Effect.logInfo("Opening WebSocket connections", {
      connections: connectionCount,
      target: url.origin,
    });
    const connections = yield* Effect.forEach(
      connectionIndexes,
      (connection) =>
        openConnection(url, connection, connectTimeoutMs).pipe(
          Effect.tap((opened) => Ref.update(openedConnections, (current) => [...current, opened])),
        ),
      { concurrency },
    );
    const connectedAt = yield* Clock.currentTimeMillis;
    yield* Effect.logInfo(
      `${connections.length} connections established in ${connectedAt - startedAt} milliseconds`,
    );
    const pingFrame = yield* encodeClientFrame({ data: "{}", event: "pusher:ping" }).pipe(
      Effect.mapError(() => loadError(0, "heartbeat", "could not encode heartbeat frame")),
    );
    yield* keepConnectionsAlive(connections, pingFrame, heartbeatSeconds).pipe(
      Effect.forever,
      Effect.forkScoped,
    );

    const sessions = yield* Effect.forEach(connections, (connection, index) => {
      const room = Math.floor(index / roomSize) + 1;
      const session = {
        ...connection,
        channel: `presence-load-${runId}-${room}`,
        userId: `load-user-${runId}-${connection.connection}`,
      };
      return encodeClientFrame({
        channel: session.channel,
        data: { sender: session.userId },
        event: clientEventName,
      }).pipe(
        Effect.map((clientEventFrame) => ({ ...session, clientEventFrame })),
        Effect.mapError(() =>
          loadError(connection.connection, "client event", "could not encode client event"),
        ),
      );
    });
    const roomMembers = new Map();
    for (const session of sessions) {
      const members = roomMembers.get(session.channel) ?? new Set();
      members.add(session.userId);
      roomMembers.set(session.channel, members);
    }

    yield* Effect.logInfo(
      `Joining ${sessions.length} clients to ${roomMembers.size} presence channels`,
    );
    yield* Effect.forEach(
      sessions,
      (session) => joinPresence(server, session, connectTimeoutMs),
      { concurrency, discard: true },
    );
    const subscribedAt = yield* Clock.currentTimeMillis;
    yield* Effect.logInfo(
      `${sessions.length} presence subscriptions active in ${subscribedAt - connectedAt} milliseconds`,
    );

    yield* Effect.logInfo("Holding all connections idle", { idleSeconds });
    yield* Effect.sleep(Duration.seconds(idleSeconds));
    const expectedDeliveries = Array.from(roomMembers.values()).reduce(
      (total, members) => total + members.size * (members.size - 1),
      0,
    );
    const exchangeStartedAt = yield* Clock.currentTimeMillis;
    yield* Effect.logInfo(
      `Sending one client event per connection and expecting ${expectedDeliveries} peer deliveries`,
    );
    let roomNumber = 0;
    for (const [channel, members] of roomMembers) {
      roomNumber += 1;
      const roomSessions = sessions.filter((session) => session.channel === channel);
      yield* Effect.logInfo(
        `Exchanging ${members.size * (members.size - 1)} peer deliveries in room ${roomNumber}/${roomMembers.size}`,
      );
      yield* exchangeClientEvents(
        roomSessions,
        new Map([[channel, members]]),
        eventTimeoutMs,
      );
    }
    const completedAt = yield* Clock.currentTimeMillis;
    yield* Effect.logInfo(
      `${expectedDeliveries} peer deliveries received in ${completedAt - exchangeStartedAt} milliseconds`,
    );
    yield* Effect.logInfo(
      `Presence connection load test passed in ${completedAt - startedAt} milliseconds`,
    );
  }),
);

NodeRuntime.runMain()(program);
