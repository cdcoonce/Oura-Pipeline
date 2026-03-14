# Dagster Modernization Plan

## Current State

The project already follows several modern patterns:

- `import dagster as dg` convention
- `@dg.definitions` decorator (lazy loading)
- `dg.EnvVar` for secrets
- `ConfigurableResource` for OuraAPI and DuckDBResource
- `@dbt_assets` with `DbtProject` and custom `DagsterDbtTranslator`

## Changes Needed

### Task 1: Pythonic Resource Injection (High Priority)

**What:** Replace `required_resource_keys` + `context.resources.xxx` with type-annotated function parameters.

**Why:** The `required_resource_keys` pattern is the legacy approach. The modern Dagster pattern uses type annotations on asset function parameters, which provides better type checking, IDE support, and is the pattern shown in all current Dagster docs.

**Before:**

```python
@dg.asset(required_resource_keys={"oura_api", "duckdb"})
def my_asset(context: dg.AssetExecutionContext) -> str:
    api: OuraAPI = context.resources.oura_api
    con = context.resources.duckdb.get_connection()
```

**After:**

```python
@dg.asset(...)
def my_asset(context: dg.AssetExecutionContext, oura_api: OuraAPI, duckdb: DuckDBResource) -> str:
    con = duckdb.get_connection()
```

**Files:**

- `src/dagster_project/defs/assets.py` — Update both `_make_daily_asset` and `_make_granular_asset` factories

---

### Task 2: Return `MaterializeResult` with Metadata (High Priority)

**What:** Replace plain string returns with `dg.MaterializeResult` containing structured metadata.

**Why:** Dagster's `MaterializeResult` surfaces metadata in the UI (row counts, timestamps, etc.) and enables asset checks and observability. Returning a plain string is the old pattern — metadata-rich results are the modern standard.

**Before:**

```python
return f"{kind}:{day} rows={count}"
```

**After:**

```python
return dg.MaterializeResult(
    metadata={
        "dagster/row_count": dg.MetadataValue.int(count),
        "partition_date": dg.MetadataValue.text(str(day)),
    }
)
```

**Files:**

- `src/dagster_project/defs/assets.py` — Both asset factories

---

### Task 3: Switch to `load_from_defs_folder` (High Priority)

**What:** Replace `load_assets_from_modules` + manual wiring in `definitions.py` with `dg.load_from_defs_folder()`, which auto-discovers all assets, resources, schedules, and checks from the `defs/` directory.

**Why:** `load_from_defs_folder` is the current recommended pattern for Dagster projects. It eliminates manual registration — any definition placed in the `defs/` folder is automatically picked up. This also means new assets, schedules, or checks don't require editing `definitions.py`.

**Changes required:**

- Move dbt asset definition into `defs/` (e.g., `defs/dbt_assets.py`)
- Move resource configuration into `defs/` as module-level `resources` (or keep in a `resources.py` that exports them)
- Replace the manual `Definitions(assets=..., resources=..., ...)` with `dg.load_from_defs_folder()`
- Each file in `defs/` exports definitions at module level; the folder scanner picks them up

**Before (`definitions.py`):**

```python
@dg.definitions
def defs():
    return dg.Definitions(
        assets=dg.load_assets_from_modules([assets]) + [dbt_model_assets],
        resources={...},
        executor=dg.in_process_executor,
    )
```

**After (`definitions.py`):**

```python
import dagster as dg

defs = dg.Definitions.load_from_defs_folder()
```

**Files:**

- `src/dagster_project/definitions.py` — Simplify to `load_from_defs_folder()`
- `src/dagster_project/defs/dbt_assets.py` (new) — Move dbt asset + DbtProject config here
- `src/dagster_project/defs/resources.py` — Export resource instances (not just classes)

---

### Task 4: Add `prepare_if_dev()` for dbt (Medium Priority, bundled with Task 3)

**What:** Call `dbt_project.prepare_if_dev()` after instantiating `DbtProject`.

**Why:** This automatically runs `dbt parse` in development to generate/refresh the manifest file. Without it, devs must manually run `dbt parse` before `dagster dev` if the manifest is stale or missing.

**Before:**

```python
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_manifest = dbt_project.manifest_path
```

**After:**

```python
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_project.prepare_if_dev()
dbt_manifest = dbt_project.manifest_path
```

**Files:**

- `src/dagster_project/definitions.py`

---

### Task 5: Add `dagster-dbt` to Dependencies (Medium Priority)

**What:** Add `dagster-dbt` to the main `dependencies` list in `pyproject.toml` (it's currently only implied through `dbt-core` in optional deps).

**Why:** The project imports `dagster_dbt` in `definitions.py` — this is a hard runtime dependency, not optional.

**Files:**

- `pyproject.toml`

---

### Task 6: Add a Daily Schedule (Medium Priority)

**What:** Add a `dg.ScheduleDefinition` or `dg.schedule` that materializes the latest partition for all raw assets daily.

**Why:** The pipeline currently has no automation — partitions must be materialized manually from the UI. A daily schedule is the standard Dagster pattern for time-partitioned pipelines.

**Implementation:**

```python
daily_oura_schedule = dg.build_schedule_from_partitioned_job(
    name="daily_oura_ingestion",
    job=dg.define_asset_job(
        name="daily_oura_job",
        selection=dg.AssetSelection.groups("oura_raw_daily", "oura_raw"),
        partitions_def=partitions,
    ),
)
```

**Files:**

- `src/dagster_project/defs/schedules.py` (new file)
- `src/dagster_project/definitions.py` — Add schedule to `Definitions`

---

### Task 7: Add Asset Checks (Low Priority)

**What:** Add basic data quality checks for key raw assets (e.g., non-null row counts, expected columns present).

**Why:** Asset checks are the Dagster-native way to validate data quality. They surface in the UI alongside assets and can block downstream execution on failure.

**Example:**

```python
@dg.asset_check(asset=dg.AssetKey(["oura_raw", "sleep"]))
def sleep_has_rows(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    con = duckdb.get_connection()
    count = con.execute("SELECT COUNT(*) FROM oura_raw.sleep").fetchone()[0]
    con.close()
    return dg.AssetCheckResult(passed=count > 0, metadata={"row_count": count})
```

**Files:**

- `src/dagster_project/defs/checks.py` (new file)
- `src/dagster_project/definitions.py` — Add checks to `Definitions`

---

## Execution Order

1. **Task 5** — Add `dagster-dbt` dependency (unblocks clean installs)
2. **Task 1** — Pythonic resource injection (core modernization)
3. **Task 2** — `MaterializeResult` with metadata (pairs naturally with Task 1)
4. **Task 3** — Switch to `load_from_defs_folder` (restructure defs/)
5. **Task 4** — Add `prepare_if_dev()` (move into dbt_assets.py during Task 3)
6. **Task 6** — Daily schedule (new functionality)
7. **Task 7** — Asset checks (new functionality, lowest priority)

## Out of Scope

- **Dagster Components (YAML DSL)** — Overkill for a small single-developer project. The Pythonic approach is explicitly supported and recommended for projects this size.
- **`dagster-pipes`** — Not relevant; this project doesn't run external processes.
- **IO Managers** — The custom DuckDB upsert pattern is appropriate for this use case; IO managers would add unnecessary abstraction.

## Testing Strategy

- Tasks 1-4 are refactors — existing tests should continue to pass after each change
- Task 5 needs a test that the schedule produces the correct run config
- Task 6 checks are tested by running against a test DuckDB instance
