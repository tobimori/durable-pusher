# Durable Object architecture

## Goals

- Keep one physical WebSocket while allowing independent subscriptions to many channels.
- Route publications by channel without a global subscriber database.
- Preserve total publication order within one channel.
- Scale storage and connection ownership by adding Durable Object instances.
- Use Durable Object SQLite as actor-local state; do not use D1 as primary state.

## Deployment topology

One Cloudflare Worker script handles public ingress and exports all six Durable Object classes:
`AppRegistry`, `ConnectionShard`, `ChannelShard`, `FanoutShard`, `ChannelDirectoryShard`, and
`UserShard`. Inter-actor calls still resolve named Durable Objects through namespace bindings, so
sharing one script does not combine storage, execution, placement, or request serialization.

Cloudflare scales the stateless ingress isolates and each named Durable Object independently. A
Worker deployment updates every actor class and disconnects active WebSockets, so clients must
reconnect and rebuild subscriptions after releases.

## Actor graph

```text
Pusher client WebSocket
        |
        v
ConnectionShard DO <-------- FanoutShard DO
        |                           ^
        | subscribe/count           | ordered publication
        v                           |
FanoutShard DO <------------- ChannelShard DO <---- signed HTTP publish
                                      |
                                      +---- ChannelDirectoryShard DO
```

### Connection shards

An ingress Worker chooses a connection shard from a configurable pool. The shard accepts the
WebSocket with the hibernation API and stores exact `(socket_id, channel)` memberships in its own
SQLite database. The WebSocket attachment contains only bounded identity and protocol metadata,
not the unbounded channel set.

A fanout call reaches a connection shard once regardless of how many local subscribers it has.
The shard selects matching sockets locally and applies socket exclusion, join barriers, and
sequence deduplication before sending frames.

### Channel shards

`idFromName(app_id + channel)` deterministically selects one channel root. It owns the publication
sequence, retry outbox, cache-channel value, branch aggregate counts, and presence state. Presence
is intentionally rooted here because Pusher limits a presence channel to 100 unique users.

The root does not retain ordinary socket or gateway membership. Its storage grows with the number
of occupied fanout branches, not the number of subscribers.

### Fanout shards

Each channel has a configurable number of deterministic fanout branches. A connection shard maps
to one branch through a stable hash. The branch stores only each gateway's current absolute
subscriber count. Absolute counts plus generation numbers make retries idempotent.

The channel root calls occupied branches, and branches call occupied connection shards in bounded
batches. Fanout cost is `O(occupied branches)` at the root and `O(occupied gateways)` across the
branches, never `O(subscribers)` in one actor.

### Directory shards

The HTTP API's occupied-channel listing is an index, not authoritative subscription state. Channel
roots update a hash-partitioned set of directory actors on occupancy changes. Individual channel
queries go directly to the channel root. This avoids a single per-app directory database while
retaining Pusher's unpaginated listing endpoint within practical response limits.

### Application placement

The global application registry resolves public keys, app IDs, and authorization tokens to an
immutable `(app_id, jurisdiction, location_hint)` placement. Ingress and every cross-actor call use
that placement when resolving a named Durable Object. Actor-local metadata pins the same identity
and rejects cross-tenant or cross-placement reuse.

Production jurisdiction routing uses Cloudflare's native restricted namespace before `getByName`;
location hints are supplied to that lookup. The local workerd emulator explicitly does not
implement jurisdiction restrictions, so the adapter falls back only when workerd returns its exact
not-implemented error. This permits local protocol tests but does not validate jurisdiction
enforcement, which still requires a Cloudflare deployment.

## Subscription barrier

1. The connection shard inserts a local `joining` membership.
2. It sends its new absolute local channel count to the fanout branch.
3. The branch updates its gateway aggregate and sends its absolute branch count to the root.
4. The root returns its latest publication sequence and channel snapshot.
5. Delivery at or below that barrier is discarded; later delivery is buffered while joining.
6. The gateway sends any cache snapshot or cache-miss event.
7. The local membership becomes `active`.
8. The gateway sends `subscription_succeeded`.
9. Buffered events are flushed in sequence order.

Unsubscribe deletes local membership before remote count propagation, so late fanout is dropped.

## Publication guarantees

The channel root allocates a monotonically increasing sequence and commits an outbox row before a
signed HTTP request succeeds. Delivery progress is tracked per branch, retried by alarm, and
removed after all current branches acknowledge it or its bounded realtime retention expires.

This gives total order per channel and at-least-once inter-object delivery. Exactly-once
WebSocket delivery is impossible because a storage write and network frame cannot be one atomic
transaction. The connection shard suppresses ordinary retry duplicates by sequence; a crash after
sending and before recording may still produce a duplicate, matching Pusher's best-effort model.

Multi-channel HTTP publication is not atomic. Reconnect does not replay missed events. Cache
channels replay only their most recent event.

## Failure repair

- Normal close handling removes all local memberships and publishes new absolute counts.
- Deployments disconnect WebSockets; official clients reconnect and rebuild state.
- Stale branch entries are corrected when a gateway reports zero active subscriptions.
- Outbox alarms retry partial fanout with per-branch acknowledgements.
- RPC envelopes and stored schemas are versioned before incompatible rollout changes.

## Scaling controls

Connection, fanout, and directory shard counts are deployment configuration, and may be increased
for new traffic without moving live WebSockets. Capacity is controlled by measured invocation
rate and fanout bytes rather than Cloudflare's maximum WebSocket count.

One globally ordered hot channel still has one sequencing root and is therefore bounded by one
Durable Object's request rate. Scaling beyond that requires publication lanes or regional roots
and intentionally weakens global ordering. No architecture can provide both an unbounded write
rate and a single total order.

## Platform limits considered

- Durable Object soft throughput: approximately 1,000 requests per second per object.
- Hibernating WebSockets: up to 32,768 per object; the operating target must be lower.
- WebSocket attachment: 16,384 bytes, so memberships stay in SQLite.
- Durable Object SQLite: 10 GB per object with unbounded paid-account aggregate storage.
- WebSocket tags: 10 per socket, so tags cannot model many-channel membership.
- Workers memory: 128 MB per isolate.

Sources:

- <https://developers.cloudflare.com/durable-objects/platform/limits/>
- <https://developers.cloudflare.com/durable-objects/best-practices/websockets/>
- <https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/>
- <https://developers.cloudflare.com/durable-objects/api/namespace/>
