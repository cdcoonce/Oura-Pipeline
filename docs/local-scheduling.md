# Local Scheduling (launchd + local Dagster)

## Why

Dagster Cloud was retired for this project. The schedules that used to run in
Dagster Serverless (`daily_oura_schedule`, `weekly_report_schedule`,
`monthly_report_schedule` in [`src/dagster_project/defs/schedules.py`](../src/dagster_project/defs/schedules.py))
now run locally on this Mac via **launchd**. launchd is the trigger; a small
Python entrypoint drives the Dagster CLI against the repo's own `.dagster`
instance.

The retired Dagster Cloud CI has been neutered accordingly: the deploy jobs in
[`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml) and
[`.github/workflows/branch_deployments.yml`](../.github/workflows/branch_deployments.yml)
were removed so CI no longer fails on the missing `DAGSTER_CLOUD_API_TOKEN`;
only the pytest jobs remain.

## What launchd runs

Each job is a launchd LaunchAgent that runs
[`scripts/run_scheduled.py`](../scripts/run_scheduled.py) with a job name. That
script:

1. Loads `.env` (existing environment wins), then **forces**
   `DAGSTER_HOME` to `<repo>/.dagster`. The committed `.env` carries a stale
   `DAGSTER_HOME` pointing at an old `~/Documents/...` path; the unconditional
   override corrects it every run.
2. Computes yesterday's date in Python (not the shell `date`, which is BSD on
   macOS) for the partitioned daily run.
3. Runs the Dagster CLI via `uv run python -m dagster ...` (the module form, not
   the `dagster` console script — so a relocated or rebuilt venv's stale
   entry-point shebang can't break an unattended run), logging to
   `logs/scheduled-<job>-<timestamp>.log` and stdout, aborting the chain if any
   step exits non-zero.

| Job       | Dagster CLI (via `uv run python -m dagster`)                                                                                | What it does                                                                                 |
| --------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `daily`   | `asset materialize --select "group:oura_raw_daily or group:oura_raw or group:staging or group:intermediate or group:marts" --partition <yesterday> -m dagster_project.definitions` | The partitioned daily refresh (raw Oura ingestion + dbt staging/marts) for yesterday's partition. Mirrors the `daily_oura_job` selection. |
| `weekly`  | `asset materialize --select weekly_health_report -m dagster_project.definitions`                                            | Generates and emails the weekly health report (previous Mon–Sun).                            |
| `monthly` | `asset materialize --select monthly_health_report -m dagster_project.definitions`                                          | Generates and emails the monthly health report (previous calendar month).                    |

> The daily run uses `asset materialize --partition` rather than
> `job execute -j daily_oura_job` because the installed Dagster CLI's
> `job execute` has no `--partition` flag. `asset materialize` applies the
> partition only to the partitioned raw assets and runs the unpartitioned dbt
> assets once — the same behavior as the `daily_oura_job` schedule. The weekly
> and monthly report assets are not partitioned.

The `weekly_report_schedule` and `monthly_report_schedule` in the code are
defined with `DefaultScheduleStatus.STOPPED`. That is intentional and fine —
launchd, not Dagster's own scheduler, is the trigger now.

## Schedule (local time)

launchd fires on the machine's local clock.

| Job       | When (local)                          | launchd `StartCalendarInterval` |
| --------- | ------------------------------------- | ------------------------------- |
| `daily`   | Every day at **06:00**                | `Hour 6, Minute 0`              |
| `weekly`  | **Mondays** at **06:30**              | `Weekday 1, Hour 6, Minute 30`  |
| `monthly` | **1st of the month** at **07:00**     | `Day 1, Hour 7, Minute 0`       |

## Install

Generate and install the LaunchAgent plists (writes to
`~/Library/LaunchAgents/com.charleslikesdata.oura.<job>.plist`):

```bash
# Write the plists but do NOT start them yet:
uv run python scripts/install_launchd.py install

# Or write AND load them into launchd so the schedules start firing:
uv run python scripts/install_launchd.py install --load
```

If you installed without `--load`, activate a job manually at any time:

```bash
launchctl load ~/Library/LaunchAgents/com.charleslikesdata.oura.daily.plist
```

Preview the exact plist XML without writing anything:

```bash
uv run python scripts/install_launchd.py dry-run
```

## Uninstall

```bash
uv run python scripts/install_launchd.py uninstall
```

This unloads each agent from launchd and removes its plist.

## Logs

- Per-run logs from the entrypoint: `logs/scheduled-<job>-<YYYYMMDD-HHMMSS>.log`
- launchd's own stdout/stderr for the agent: `logs/launchd-<job>.out.log` and
  `logs/launchd-<job>.err.log`

`logs/` is gitignored, so nothing here is committed.

## Dagster UI

The Dagster web UI is still available locally — launchd does not replace it:

```bash
uv run dagster dev
```

This serves the UI (default <http://localhost:3000>) against the same
`.dagster` instance, so scheduled runs launched by launchd show up there.

## First run: dbt manifest

The dbt-backed assets (`src/dagster_project/defs/dbt_assets.py`) need a compiled
dbt manifest, which is generated on the first dbt invocation. If the code
location complains about a missing manifest, generate it once by running a job
manually or by starting the UI:

```bash
# Either run the daily job once by hand...
uv run python scripts/run_scheduled.py daily
# ...or just start the UI once, which parses/builds the project:
uv run dagster dev
```

After that, the manifest exists and the launchd runs work unattended.

## Network & credentials

These jobs are **not** offline:

- **`daily`** hits the **live Oura API** (OAuth) to ingest raw data and writes
  to **Snowflake** via dbt. It needs valid `OURA_*` and `SNOWFLAKE_*` values in
  `.env` and network access.
- **`weekly`** and **`monthly`** read from Snowflake and **send email via AWS
  SES**. They need `SNOWFLAKE_*`, `SES_SENDER_EMAIL`, `SES_RECIPIENT_EMAIL`,
  `AWS_REGION`, and working AWS credentials in the environment.

Because launchd agents run with a minimal environment, `scripts/run_scheduled.py`
loads `.env` itself so these credentials are present. Make sure `.env` is
populated (see [`.env.example`](../.env.example)) before enabling the schedules.
