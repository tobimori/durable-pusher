# Pusher Channels compatibility notes

Research date: 2026-08-21.

## Connection model

Official clients connect to `/app/{app_key}` and supply `protocol`, `client`, and `version` query
parameters. Current client families require protocol versions 5, 6, and 7. A successful upgrade
receives a double-encoded handshake:

```json
{
  "event": "pusher:connection_established",
  "data": "{\"socket_id\":\"1234.5678\",\"activity_timeout\":120}"
}
```

The socket ID must match `^\d+\.\d+$`. Each reconnect receives a new ID and recreates all server
state. Clients retain their requested channel set, then resubscribe and reauthorize after the new
handshake.

A connection is independent from a channel. One WebSocket sends any number of
`pusher:subscribe` and `pusher:unsubscribe` commands. The server must keep a per-socket channel set;
it cannot assign the socket itself to a channel actor.

## Message encoding

Every protocol message is a UTF-8 WebSocket text frame with an `event` string. Server application
events always carry string-valued `data`, including when the application payload is JSON:

```json
{
  "event": "order-updated",
  "channel": "private-orders",
  "data": "{\"id\":42}"
}
```

This double encoding is required by Swift and preserves the behavior of JavaScript, Java, .NET,
and Objective-C clients. `pusher:error` is the notable exception and carries an object-valued
`data` field.

## Subscription behavior

Public channels need no signature. Private, presence, and encrypted channel signatures are
lowercase hexadecimal HMAC-SHA256 values prefixed by the app key:

```text
private:  socket_id:channel_name
presence: socket_id:channel_name:exact_channel_data_string
```

Presence snapshots count unique users, while ordinary subscription counts count connections. A
user with several sockets emits `member_added` on its first join and `member_removed` after its
last departure. Duplicate subscribe and unsubscribe commands are idempotent.

Encrypted channels use the same WebSocket subscription protocol as private channels. Official
server SDKs encrypt event data before calling the HTTP API, so the realtime server forwards the
encrypted `nonce` and `ciphertext` JSON string without decrypting it.

Cache channels retain one server-published event for at most 30 minutes. A cache hit is delivered
before `pusher_internal:subscription_succeeded`; a miss produces `pusher:cache_miss`.

Client events are accepted only when enabled, named `client-*`, and sent by a subscribed private
or presence socket. They are limited to 10 events per second per connection and are not echoed to
their originating socket.

## Heartbeats and errors

The server answers `pusher:ping` with `pusher:pong`; Java clients may omit the ping's `data` field.
Native RFC 6455 ping/pong remains enabled independently. The advertised activity timeout is 120
seconds.

Protocols below 6 receive a `pusher:error` frame before a fatal close. Protocols 6 and 7 use the
WebSocket close code directly. Important fatal codes include invalid app `4001`, unsupported
protocol `4007`, missing protocol `4008`, over-capacity retry `4100`, immediate reconnect `4200`,
and heartbeat timeout `4201`.

## HTTP API

Official server libraries sign requests to `/apps/{app_id}/...` with these query parameters:

```text
auth_key
auth_timestamp
auth_version=1.0
body_md5
auth_signature
```

The signature input is:

```text
UPPERCASE_METHOD + "\n" + REQUEST_PATH + "\n" + SORTED_DECODED_QUERY_WITHOUT_SIGNATURE
```

`body_md5` is the lowercase MD5 of the exact body bytes. `auth_signature` is lowercase
HMAC-SHA256. Timestamps must be less than 600 seconds from server time. Successful mutation
responses use HTTP 200 and a JSON body, at minimum `{}`, because several official SDKs always
decode the response.

Current endpoints are events, batch events, occupied channel queries, presence user queries, and
user connection termination. A single publish targets at most 100 channels, a batch contains at
most 10 events, and event data is at most 10 KB.

## Application control API

The service-specific control API is authenticated with
`Authorization: Bearer <PUSHER_CONTROL_TOKEN>` and exposes:

```text
GET    /control/v1/apps
POST   /control/v1/apps
GET    /control/v1/apps/{app_id}
PATCH  /control/v1/apps/{app_id}
DELETE /control/v1/apps/{app_id}
```

Creation returns the generated app secret, authorization token, and encryption key exactly once.
Subsequent reads return only application metadata and the public app key. Jurisdiction and
location hints are selected at creation and are immutable because changing either value does not
relocate existing named Durable Objects. Deletion leaves an app ID tombstone, so an ID cannot be
reused for another tenant.

## Compatibility surface

Direct protocol test lanes are pusher-js (protocol 7), Java/Android (5), Swift (7 and WebSocket
subprotocol negotiation), .NET/Unity (5), and legacy Objective-C (6). Browser XHR streaming,
polling, and SockJS are separate fallback transports and are not part of WebSocket protocol
compatibility.

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
