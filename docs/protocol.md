# Pusher Channels compatibility notes

Research date: 2026-08-21.

## Connection model

Official clients connect to `/app/{app_key}`. They supply the `protocol`, `client`, and `version`
query parameters. The service accepts protocol versions 5, 6, and 7.

After a successful upgrade, the server sends this handshake. The `data` value is an encoded JSON
string.

```json
{
  "event": "pusher:connection_established",
  "data": "{\"socket_id\":\"1234.5678\",\"activity_timeout\":120}"
}
```

The socket ID must match `^\d+\.\d+$`. The server gives a new ID to each reconnection. The server
does not keep state from the old connection. The client keeps its requested channel list. After the
new handshake, the client subscribes and authorizes again.

A connection is independent of a channel. One WebSocket can send multiple `pusher:subscribe` and
`pusher:unsubscribe` commands. The server keeps a channel list for each socket. It does not assign
the socket to one channel object.

## Message encoding

Each protocol message is a UTF-8 WebSocket text frame. Each message has an `event` string. Server
application events always have a string in `data`. This rule also applies when the application data
is JSON.

```json
{
  "event": "order-updated",
  "channel": "private-orders",
  "data": "{\"id\":42}"
}
```

Swift clients require this double encoding. JavaScript, Java, .NET, and Objective-C clients also
support it. The `pusher:error` event is different. Its `data` field contains an object.

## Subscription behavior

Public channels do not require a signature. Private, presence, and encrypted channels require a
signature. The signature starts with the application key. The signature value is a lowercase
hexadecimal HMAC-SHA256 value.

```text
private:  socket_id:channel_name
presence: socket_id:channel_name:exact_channel_data_string
```

A presence snapshot counts unique users. An ordinary subscription count counts connections. The
server sends `member_added` when the first connection of a user joins. It sends `member_removed`
after the last connection of the user leaves.

The server accepts duplicate subscribe and unsubscribe commands. A duplicate command does not
change the result.

Encrypted channels use the private-channel WebSocket subscription protocol. Official server SDKs
encrypt event data before they call the HTTP API. The realtime server forwards the encrypted JSON
string. It does not decrypt the `nonce` or `ciphertext`.

A cache channel keeps one server event for a maximum of 30 minutes. For a cache hit, the server sends
the event before `pusher_internal:subscription_succeeded`. For a cache miss, the server sends
`pusher:cache_miss`.

The server accepts client events only when the application enables them. A client event name must
start with `client-`. A subscribed private or presence socket must send the event. Each connection
can send a maximum of 10 client events each second. The server does not send the event back to its
source socket.

## Heartbeats and errors

The server answers `pusher:ping` with `pusher:pong`. Java clients can omit the `data` field from the
ping. Native RFC 6455 ping and pong frames operate independently. The server specifies an activity
timeout of 120 seconds.

For protocol version 5, the server sends `pusher:error` before a fatal close. For protocol versions 6
and 7, the server uses the WebSocket close code.

| Code   | Cause                         |
| ------ | ----------------------------- |
| `4001` | The application is invalid    |
| `4007` | The protocol is not supported |
| `4008` | The protocol is missing       |
| `4100` | The client must retry later   |
| `4200` | The client must reconnect now |
| `4201` | The heartbeat timed out       |

## HTTP API

Official server libraries sign requests to `/apps/{app_id}/...`. A signed request has these query
parameters:

```text
auth_key
auth_timestamp
auth_version=1.0
body_md5
auth_signature
```

Use this string as the signature input:

```text
UPPERCASE_METHOD + "\n" + REQUEST_PATH + "\n" + SORTED_DECODED_QUERY_WITHOUT_SIGNATURE
```

`body_md5` is the lowercase MD5 value of the exact body bytes. `auth_signature` is a lowercase
HMAC-SHA256 value. The difference between the timestamp and server time must be less than 600
seconds.

A successful change request returns HTTP 200 and a JSON body. The smallest valid body is `{}`.
Several official SDKs always decode this body.

The API has endpoints for these operations:

- Publish one event
- Publish a batch of events
- Get occupied channels
- Get presence users
- Terminate user connections

One publish operation can target a maximum of 100 channels. One batch can contain a maximum of 10
events. Event data can have a maximum size of 10 KB.

## Application control API

Use this header to authenticate with the service control API:

```text
Authorization: Bearer <PUSHER_CONTROL_TOKEN>
```

The control API has these endpoints:

```text
GET    /control/v1/apps
POST   /control/v1/apps
GET    /control/v1/apps/{app_id}
PATCH  /control/v1/apps/{app_id}
DELETE /control/v1/apps/{app_id}
```

Application creation returns the generated secret, authorization token, and encryption key. The
service returns these values only one time. Later reads return application data and the public
application key.

Select the jurisdiction and location hint when you create the application. You cannot change these
values. A change cannot move existing named Durable Objects. Application deletion keeps an ID
tombstone. Therefore, you cannot use the ID for a different tenant.

## Tested clients

Direct protocol tests use these clients:

- pusher-js with protocol 7
- Java and Android with protocol 5
- Swift with protocol 7 and WebSocket subprotocol negotiation
- .NET and Unity with protocol 5
- Legacy Objective-C with protocol 6

Browser XHR streaming, polling, and SockJS are fallback transports. The WebSocket compatibility
tests do not include these transports.

## Primary sources

- <https://pusher.com/docs/channels/library_auth_reference/pusher-websockets-protocol/>
- <https://pusher.com/docs/channels/library_auth_reference/rest-api/>
- <https://pusher.com/docs/channels/library_auth_reference/auth-signatures/>
- <https://pusher.com/docs/channels/using_channels/presence-channels/>
- <https://pusher.com/docs/channels/using_channels/encrypted-channels/>
- <https://pusher.com/docs/channels/using_channels/cache-channels/>
- <https://github.com/pusher/pusher-js>
- <https://github.com/pusher/pusher-websocket-java>
- <https://github.com/pusher/pusher-websocket-swift>
- <https://github.com/pusher/pusher-websocket-dotnet>
- <https://github.com/pusher/pusher-http-node>
