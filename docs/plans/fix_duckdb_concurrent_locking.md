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

**Root cause:** `DuckDBResource.get_connection()` (resources.py:146-149) creates
a fresh `duckdb.connect()` on every call with no coordination between callers.

## Options Considered

### Option A — Serialize via a shared, reusable connection (recommended)

Change `DuckDBResource` to hold **one long-lived connection** and hand out the
same connection object to every asset. DuckDB's Python connection is
thread-safe for sequential statement execution, and Dagster's in-process
executor already runs ops in a single process.

**Pros:**

- Simplest change (~15 lines).
- Eliminates the lock error entirely.
- No new dependencies.

**Cons:**

- If Dagster is ever configured with a multi-process executor, a single
  in-memory connection object cannot be shared across processes. (Currently not
  an issue — this pipeline uses the default in-process executor.)

### Option B — Retry with backoff

Wrap `get_connection()` in a retry loop that catches `IOException` and sleeps
before retrying.

**Pros:**

- No architectural change.

**Cons:**

- Adds latency; still fails under heavy concurrency.
- Masks the real problem rather than solving it.
- Fragile — retry window is a guess.

### Option C — Dagster concurrency limits / run queue

Configure Dagster to run only one op at a time using `tag_concurrency_limits`
or a run queue with `max_concurrent_runs: 1`.

**Pros:**

- Zero code change.

**Cons:**

- Kills all parallelism, including the API-fetch phase that doesn't need the
  DB. Materializing a year of daily partitions becomes 13× slower than
  necessary.

### Option D — Connection pool with mutex

Use `threading.Lock` to serialize only the `connect()` / write phase, allowing
API fetches to still run in parallel.

**Pros:**

- Preserves API-fetch parallelism.

**Cons:**

- More complex than Option A for the same end result in an in-process executor
  (Dagster already serializes op execution within a single run unless you opt
  into multiprocess).

## Chosen Approach — Option A (shared connection)

A single long-lived connection is the right fit because:

1. The pipeline uses Dagster's default **in-process executor** (single process,
   single thread per op step).
2. DuckDB connections are lightweight — holding one open for the duration of a
   run has negligible cost.
3. It directly eliminates the conflicting-lock error with minimal code change.

## Implementation Plan

### Task 1 — Refactor `DuckDBResource` to use a shared connection

**File:** `src/dagster_project/defs/resources.py`

- Add `_connection` class-level attribute (default `None`).
- In `get_connection()`, return `self._connection` if it already exists and is
  not closed; otherwise create, cache, and return a new connection.
- Add a `close()` method that closes and clears the cached connection.
- Keep the `CREATE SCHEMA IF NOT EXISTS oura_raw` call (idempotent, fine to
  run once).

```python
class DuckDBResource(dg.ConfigurableResource):
    """DuckDB connection provider — reuses a single connection per resource
    instance to avoid file-lock conflicts under concurrent execution."""

    db_path: str = "data/oura.duckdb"
    _connection: duckdb.DuckDBPyConnection | None = None

    model_config = {"arbitrary_types_allowed": True}

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None or self._connection.connection is None:
            self._connection = duckdb.connect(self.db_path)
            self._connection.execute("CREATE SCHEMA IF NOT EXISTS oura_raw;")
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
```

### Task 2 — Remove per-asset `con.close()` calls

**File:** `src/dagster_project/defs/assets.py`

Both `_make_daily_asset` and `_make_granular_asset` currently call `con.close()`
after each upsert. With a shared connection this would break subsequent assets.
Remove these `con.close()` lines (lines 196 and 227 approx).

### Task 3 — Add / update tests

**File:** `tests/test_duckdb_resource.py` (new)

- Test that two successive `get_connection()` calls return the **same**
  connection object.
- Test that `close()` resets the cached connection so the next
  `get_connection()` opens a fresh one.
- Test that calling `get_connection()` after `close()` works (re-creates).

### Task 4 — Manual verification

- Run `uv run dagster dev` and materialize 3+ partitions of different assets
  concurrently.
- Confirm no `IOException` / lock errors.

## Risk & Rollback

- **Risk:** If Dagster is later switched to a multiprocess executor, the shared
  connection won't be shared across OS processes (each process gets its own
  resource instance). In that scenario the lock error would return and we'd need
  to revisit with Option D or a connection-pooling approach.
- **Rollback:** Revert the two file changes; the old per-call connect behavior
  is restored.
