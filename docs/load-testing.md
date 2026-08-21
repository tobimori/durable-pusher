# Connection load testing

The connection load smoke test opens 1,000 WebSockets by default and retains all of them
concurrently. It assigns the clients to presence channels of up to 100 members, authenticates and
subscribes every client, holds them idle, then makes every client send one `client-*` event. Every
other member of that presence channel must receive the event before the test passes.

The default ten rooms produce 99,000 verified peer deliveries. Rooms are capped at 100 unique users
to preserve Pusher Channels presence compatibility. All 1,000 sockets remain connected while the
rooms exchange events sequentially, avoiding an emulator-only alarm bottleneck without reducing
the simultaneous connection count. A staggered Pusher protocol heartbeat keeps long-running tests
within the server's advertised activity timeout.

Start the local stack, then run:

```sh
PUSHER_E2E_PORT=1337 mise exec node@24 -- pnpm test:load:connections
```

The target and load shape are configurable:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `PUSHER_APP_KEY` | `local-key` | Application key used by every connection |
| `PUSHER_APP_ID` | `local-app` | Application ID used for presence authorization |
| `PUSHER_APP_SECRET` | `local-secret` | Application secret used for presence authorization |
| `PUSHER_E2E_HOST` | `127.0.0.1` | Target hostname |
| `PUSHER_E2E_PORT` | `1337` | Target port |
| `PUSHER_E2E_TLS` | `0` | Use `wss` when set to `1` |
| `PUSHER_LOAD_CONNECTIONS` | `1000` | Simultaneous retained connections |
| `PUSHER_LOAD_CONCURRENCY` | `100` | Maximum concurrent connection attempts |
| `PUSHER_LOAD_CONNECT_TIMEOUT_MS` | `30000` | Establishment and subscription timeout per socket |
| `PUSHER_LOAD_EVENT_TIMEOUT_MS` | `300000` | Timeout for each presence room's client-event exchange |
| `PUSHER_LOAD_HEARTBEAT_SECONDS` | `60` | Interval for each connection's staggered protocol ping |
| `PUSHER_LOAD_IDLE_SECONDS` | `5` | Idle hold before every client sends an event |
| `PUSHER_LOAD_PRESENCE_ROOM_SIZE` | `100` | Members per presence channel, maximum 100 |

This is a repeatable load smoke test, not a capacity benchmark. Local workerd can validate the
hibernatable WebSocket API path but cannot prove that Cloudflare evicted and later restored a
specific Durable Object. Production limits and hibernation behavior require the same test against
a deployed Worker with Cloudflare metrics enabled.
