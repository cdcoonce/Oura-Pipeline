#!/usr/bin/env python3
"""launchd entrypoint for the local Oura pipeline schedule.

Runs a named scheduled job (daily / weekly / monthly) by shelling out to the
Dagster CLI via ``uv run dagster ...``. This replaces the retired Dagster Cloud
schedules -- launchd triggers this script, and this script drives Dagster
locally against the repo's own ``.dagster`` instance.

Dependency-free (stdlib only) so it runs under the macOS system ``python3``
(3.9+) that launchd invokes -- no project virtualenv required. The actual
Dagster execution happens inside ``uv run``, which resolves the project's own
(newer) environment.

Usage
-----
    python scripts/run_scheduled.py daily
    python scripts/run_scheduled.py weekly
    python scripts/run_scheduled.py monthly
"""

from __future__ import annotations

import argparse
import datetime
import os
import shutil
import subprocess
from datetime import timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path) -> None:
    """Minimal ``.env`` parser -- existing environment wins.

    For each line: skip blanks, ``#`` comments, and lines without ``=``. Split
    on the first ``=``; strip whitespace and one layer of surrounding single or
    double quotes from the value. A leading ``export`` on the key is stripped.
    Uses ``os.environ.setdefault`` so anything already exported (by launchd or
    the invoking user) takes precedence over the file.
    """
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export ") :].strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def main() -> int:
    load_dotenv(REPO / ".env")

    # The repo .env may carry a stale DAGSTER_HOME (it was authored when the
    # repo lived under a different path). Override it UNCONDITIONALLY so the
    # runner always targets this repo's own .dagster instance. This must be a
    # hard assignment, not setdefault, precisely because .env may have set it.
    os.environ["DAGSTER_HOME"] = str(REPO / ".dagster")
    (REPO / ".dagster").mkdir(parents=True, exist_ok=True)
    (REPO / "logs").mkdir(parents=True, exist_ok=True)

    # Compute yesterday in Python -- never shell `date`, which is BSD on macOS
    # and does not accept the GNU `-d yesterday` form. Computed in UTC to match
    # the assets' DailyPartitionsDefinition and the retired schedules'
    # execution_timezone, so the key is valid regardless of the machine's tz.
    # NB: datetime.timezone.utc (not datetime.UTC) — this stdlib-only script runs
    # under the macOS system python3 (3.9), which predates the datetime.UTC alias
    # (3.11+). ruff's UP017 assumes a newer interpreter, so it's suppressed.
    yesterday = (
        datetime.datetime.now(datetime.timezone.utc).date()  # noqa: UP017
        - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    uv = shutil.which("uv") or str(Path.home() / ".local/bin/uv")

    module = "dagster_project.definitions"

    # Each job is an ordered list of dagster CLI argv lists. The runner prepends
    # [uv, "run", "dagster"] to each step and executes them in order, aborting
    # the chain on the first non-zero exit.
    #
    # daily  -> the partitioned daily_oura_job's asset selection, materialized
    #           for yesterday's partition. daily_oura_job selects five asset
    #           groups (raw ingestion + dbt staging/intermediate/marts). The
    #           `job execute` CLI has no --partition flag in this Dagster
    #           version, so we drive the same selection via `asset materialize`,
    #           which does support --partition and correctly applies it only to
    #           the partitioned raw assets while running the unpartitioned dbt
    #           assets once.
    # weekly  -> materialize the non-partitioned weekly_health_report asset.
    # monthly -> materialize the non-partitioned monthly_health_report asset.
    daily_selection = (
        "group:oura_raw_daily or group:oura_raw or group:staging "
        "or group:intermediate or group:marts"
    )
    jobs = {
        "daily": [
            [
                "asset",
                "materialize",
                "--select",
                daily_selection,
                "--partition",
                "{partition}",
                "-m",
                module,
            ],
        ],
        "weekly": [
            [
                "asset",
                "materialize",
                "--select",
                "weekly_health_report",
                "-m",
                module,
            ],
        ],
        "monthly": [
            [
                "asset",
                "materialize",
                "--select",
                "monthly_health_report",
                "-m",
                module,
            ],
        ],
    }

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", choices=sorted(jobs))
    args = parser.parse_args()

    steps = jobs[args.job]

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = REPO / "logs" / f"scheduled-{args.job}-{stamp}.log"

    def emit(msg: str, log) -> None:
        """Write a timestamped line to both the log file and stdout."""
        line = f"{datetime.datetime.now().isoformat(timespec='seconds')} {msg}"
        print(line, flush=True)
        log.write(line + "\n")
        log.flush()

    with open(log_path, "a", buffering=1, encoding="utf-8") as log:
        emit(f"START job={args.job} partition={yesterday} dagster_home={os.environ['DAGSTER_HOME']}", log)
        emit(f"log_file={log_path}", log)
        for i, step in enumerate(steps, start=1):
            resolved = [part.format(partition=yesterday) for part in step]
            # Invoke via ``python -m dagster`` (not the ``dagster`` console
            # script), which is immune to a stale entry-point shebang left by a
            # relocated/rebuilt venv -- the exact failure mode this repo hit.
            argv = [uv, "run", "python", "-m", "dagster", *resolved]
            emit(f"STEP {i}/{len(steps)}: {' '.join(argv)}", log)
            proc = subprocess.run(
                argv,
                cwd=REPO,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            log.flush()
            if proc.returncode != 0:
                emit(f"FAILED (exit {proc.returncode}) at step {i}/{len(steps)}", log)
                return proc.returncode
        emit(f"DONE job={args.job} (all {len(steps)} step(s) succeeded)", log)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
