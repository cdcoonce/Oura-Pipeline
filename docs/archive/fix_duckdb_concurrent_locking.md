# Fix DuckDB Concurrent Locking

## Problem

When materializing multiple partitions (or multiple assets) concurrently, each
asset calls `DuckDBResource.get_connection()`, which opens a **new exclusive
connection** via `duckdb.connect(db_path)`. DuckDB's file-based storage only
permits a single writer at a time, so the second concurrent asset immediately
fails with:

```
_duckdb.IOException: IO Error: Could not set lock on file
"data/oura.duckdb": Conflicting lock is held ...
```

**Root cause:** Dagster launches separate runs (separate processes) for each
partition. Each process creates its own `DuckDBResource` instance and opens its
own `duckdb.connect()`, causing file-level lock contention.

## Options Considered

### Option A — Shared connection within a resource instance

Cache a single connection on the `DuckDBResource` and reuse it across assets.

**Rejected:** Dagster creates separate resource instances per run (per process),
so the cached connection is not shared across concurrent partition runs.

### Option B — Retry with backoff

Wrap `get_connection()` in a retry loop.

**Rejected:** Adds latency, masks the real problem, fragile under heavy
concurrency.

### Option C — Dagster run queue (chosen)

Set `max_concurrent_runs: 1` in `dagster.yaml` so Dagster queues partition runs
and executes them sequentially.

### Option D — Connection pool with mutex

Use `threading.Lock` to serialize the write phase.

**Rejected:** Doesn't help across separate OS processes.

## Chosen Approach — Option C (run queue)

**File:** `.dagster/dagster.yaml`

```yaml
run_queue:
  max_concurrent_runs: 1
```

This is the right fit because:

1. The lock conflict is between **separate Dagster run processes**, not threads.
2. Each individual run already uses `in_process_executor` (sequential within a
   run), so intra-run parallelism is unaffected.
3. Zero code changes required — purely configuration.
4. The pipeline is a personal data pipeline with 13 assets; sequential runs add
   negligible wall-clock time since the API calls are the bottleneck, not the
   DB writes.

## Implementation

Added `run_queue.max_concurrent_runs: 1` to `.dagster/dagster.yaml`. This file
is gitignored (instance-local config), so no code changes are committed.

## Verification

- Run `dagster dev` and materialize 3+ partitions of different assets.
- Confirm runs are queued (visible in the Dagster UI runs tab).
- Confirm no `IOException` / lock errors.
