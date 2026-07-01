# Milestone 1: Standalone Unit Write Thread Model

Milestone 1 is scoped to a single `chronicle-unit` accepting writes directly through the existing gRPC `Record` stream. It does not require catalog-backed timeline creation or registration, admin APIs, metrics, retention, unit-side reads, ensemble replication, segment replacement, or client-side distributed recovery.

The goal is to make the unit-local write path internally consistent and restart-safe before layering the distributed timeline writer on top.

## Scope

In scope:

- Accept `RecordEventsRequest` batches on one unit.
- Validate term fencing before persistence.
- Persist each accepted event to the WAL.
- Acknowledge only after the WAL has been synced and the event is visible in the write cache.
- Replay durable WAL entries into the write cache on unit restart.
- Remove the read and write actor layer from the unit service.
- Keep the unit server write-only; the gRPC `Fetch` method remains in the proto/client surface but returns `UNIMPLEMENTED` from the unit.
- Keep stream coroutine shutdown finite and controlled by the unit server.

Out of scope for this milestone:

- Catalog timeline lifecycle.
- Catalog registration and load reporting.
- Admin service APIs.
- Metrics and Prometheus export.
- Retention management.
- Ensemble selection or replacement.
- Cross-unit replication.
- Client-side `StateMachine` reconciliation.
- Unit-side read serving.

## Thread Model

The standalone unit write path has six active components:

1. `UnitService::record`
   Receives gRPC request batches and creates per-item response channels.

2. Per-record-stream receive coroutine
   Reads request batches, checks term, reserves bounded in-flight capacity, encodes full events, and appends them to the WAL.

3. `Wal` writer task
   Batches byte records, writes them to the current WAL segment, and publishes the advanced WAL byte offset.

4. `Wal` sync task
   Syncs the segment and publishes the durable WAL byte offset.

5. Per-record-stream sync coroutine
   Watches the durable WAL offset, drains synced in-flight writes into the write cache, and completes response callbacks.

6. Unit service task tracker
   Owns spawned stream tasks so `Unit::stop` can cancel and join them.

The intended flow is:

```text
Record stream
  -> UnitService batch loop
  -> receive coroutine term check
  -> bounded in-flight reservation
  -> Wal::append(encoded Event)
  -> WAL writer writes Record(encoded Event)
  -> WAL syncer publishes durable byte offset
  -> sync coroutine puts Event in WriteCache
  -> UnitService sends RecordEventsResponse
```

`UnitService` tracks every stream task it spawns. `Unit::stop` cancels the unit context, stops accepting new streams, then waits for tracked stream tasks before shutting down the WAL.

## Data Model

The WAL payload for a write must be the encoded `pb_ext::Event`, not just the event payload bytes.

```text
WAL Record.data == prost_encode(Event)
WriteCache entry == same Event
Ack offset == Event.offset
WAL byte offset == internal durability cursor only
```

This distinction matters:

- `Event.offset` is the logical stream offset returned to clients.
- The WAL byte offset is only used to determine when a write is durable.
- Recovery decodes WAL records as `Event`, so WAL records must contain full events.

## State Model

For a single unit, the write thread state can be modeled as:

```text
term_by_timeline: timeline_id -> term
lra_by_timeline: timeline_id -> logical offset
wal_pending: ordered list of encoded events not yet durably synced
wal_durable: ordered list of encoded events durably synced
inflight: ordered list of (wal_byte_offset, event, response_channel)
write_cache: timeline_id, offset -> event
```

Valid state transitions:

1. `Fence(timeline, new_term)`
   If `new_term > current_term`, update the term and return current LRA. Otherwise reject.

2. `ReceiveWrite(event)`
   If `event.term < current_term`, reject before WAL append. Otherwise enqueue the encoded event to the WAL and track it as in-flight.

3. `WalSynced(byte_offset)`
   For every in-flight write with `wal_byte_offset <= byte_offset`, insert the event into the write cache, update LRA if applicable, and acknowledge the logical event offset.

4. `Restart`
   Rebuild `write_cache` from `wal_durable` before accepting new writes.

## Invariants

Milestone 1 implementation should satisfy these invariants:

- No ack before durability: every acknowledged event was synced to the WAL first.
- No ack before visibility: every acknowledged event is present in the write cache.
- WAL/cache agreement: replaying durable WAL records produces the same events that were acknowledged.
- Term safety: stale-term writes are rejected before WAL append.
- Logical offset clarity: client-visible offsets come from `Event.offset`, not WAL byte offsets.
- Backpressure propagation: if the write cache cannot accept synced events, the sync coroutine stops draining in-flight writes; the in-flight channel fills; the receive coroutine stops polling the gRPC request stream.
- Finite shutdown: stopping the unit cancels stream coroutines and waits for them to exit.

## Implementation Status

Implemented in this milestone pass:

- Removed the read and write actor layer from `chronicle-unit`.
- Removed unit-side read serving; `Fetch` now returns `UNIMPLEMENTED` on the unit.
- Removed unit-side admin service, metrics/Prometheus setup, retention manager, and catalog registration/reporting.
- `UnitService::record` now creates a per-stream receive path and a per-stream WAL-sync apply path.
- Accepted writes append encoded full `Event` records to the WAL.
- WAL recovery opens existing segments without truncating them and replays full `Event` records into the write cache.
- `UnitService` tracks spawned stream tasks through `UnitServiceTasks`; `Unit::stop` cancels and joins them before shutting down the compaction pipeline and WAL.
- Stream sync cancellation is aware of write-cache backpressure, so blocked cache application exits during unit shutdown.

## Acceptance Tests

Covered by focused tests:

- A valid write is acknowledged only after WAL sync and then appears in `WriteCache`.
- A stale-term write is rejected and does not append to WAL.
- WAL replay after reopening the unit recovers full `Event` records.
- Cache-backpressured sync work exits on cancellation, which protects unit shutdown from hanging on stream tasks.

## Implementation Order

1. Replace write actors with direct record-stream receive/sync coroutines.
2. Add WAL open modes so recovery can read existing segments without truncating them.
3. Remove unit-side read serving while leaving the proto/client fetch surface untouched.
4. Remove admin, metrics, retention, and catalog registration from the standalone unit.
5. Add unit-service task tracking and graceful stream-task shutdown.
6. Add focused WAL replay and standalone write tests.
7. Re-run `cargo fmt --all --check`, `cargo check --workspace --all-targets`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace`.
