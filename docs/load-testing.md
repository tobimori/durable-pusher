# Connection load testing

By default, the load smoke test opens 1,000 WebSockets. The test keeps all sockets open at the same
time. It puts the clients in presence channels. You can set the number of clients in each channel.

The test authenticates and subscribes each client. It then keeps the clients idle for a specified
time. Next, each client sends one `client-*` event. All other members of the channel must receive the
event. The test fails if a member does not receive the event.

The default configuration uses 10 rooms. It verifies 99,000 event deliveries. All 1,000 sockets stay
connected during the test. The test processes one room at a time. This process prevents an alarm
limit in the local emulator. It does not decrease the number of open connections.

The test sends Pusher heartbeat messages at different times. These messages keep long tests within
the server activity timeout.

Start the local service. Then, run this command:

```sh
PUSHER_E2E_PORT=1337 mise exec node@24 -- pnpm test:load:connections
```

Use these variables to set the target and load:

| Variable                         |        Default | Function                                      |
| -------------------------------- | -------------: | --------------------------------------------- |
| `PUSHER_APP_KEY`                 |    `local-key` | Sets the application key for all connections  |
| `PUSHER_APP_ID`                  |    `local-app` | Sets the application ID                       |
| `PUSHER_APP_SECRET`              | `local-secret` | Sets the secret for presence authorization    |
| `PUSHER_E2E_HOST`                |    `127.0.0.1` | Sets the target host                          |
| `PUSHER_E2E_PORT`                |         `1337` | Sets the target port                          |
| `PUSHER_E2E_TLS`                 |            `0` | Uses `wss` if the value is `1`                |
| `PUSHER_LOAD_CONNECTIONS`        |         `1000` | Sets the number of open connections           |
| `PUSHER_LOAD_CONCURRENCY`        |          `100` | Limits simultaneous connection attempts       |
| `PUSHER_LOAD_CONNECT_TIMEOUT_MS` |        `30000` | Sets the timeout for connection and subscribe |
| `PUSHER_LOAD_EVENT_TIMEOUT_MS`   |       `300000` | Sets the event timeout for each presence room |
| `PUSHER_LOAD_HEARTBEAT_SECONDS`  |           `60` | Sets the heartbeat interval for each client   |
| `PUSHER_LOAD_IDLE_SECONDS`       |            `5` | Sets the idle time before clients send events |
| `PUSHER_LOAD_PRESENCE_ROOM_SIZE` |          `100` | Sets the number of members in a presence room |

Do not use this smoke test as a capacity benchmark. Local workerd can test the hibernatable WebSocket
API. It cannot verify that Cloudflare removed and restored a specified Durable Object.

A room size greater than 100 tests behavior beyond the documented Pusher presence limit. To test
production limits and hibernation, run the test against a deployed Worker. Enable Cloudflare metrics
for that test.
