#!/usr/bin/env python3
"""Generate and (optionally) install launchd agents for the Oura schedule.

Replaces the retired Dagster Cloud schedules with macOS ``launchd`` LaunchAgents
that invoke ``scripts/run_scheduled.py`` on a calendar interval. Plists are
generated in Python via the stdlib ``plistlib`` -- there is no separate template
file to keep in sync.

Subcommands
-----------
    install [--load]   Write the plists to ~/Library/LaunchAgents. With --load,
                       also (re)load them into launchd so they start firing.
    uninstall          Unload and remove the plists.
    dry-run            Print the generated plist XML for each job. Writes nothing.

Nothing is ever loaded into launchd unless ``--load`` is passed explicitly.
"""

from __future__ import annotations

import argparse
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LABEL_PREFIX = "com.charleslikesdata.oura"
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"

# StartCalendarInterval per job, in LOCAL time. launchd fires on the local clock.
#   daily   -> every day at 06:00
#   weekly  -> Mondays (Weekday 1) at 06:30
#   monthly -> the 1st of the month at 07:00
SCHEDULES: dict[str, dict[str, int]] = {
    "daily": {"Hour": 6, "Minute": 0},
    "weekly": {"Weekday": 1, "Hour": 6, "Minute": 30},
    "monthly": {"Day": 1, "Hour": 7, "Minute": 0},
}


def label_for(job: str) -> str:
    """Reverse-DNS launchd label for a job, e.g. com.charleslikesdata.oura.daily."""
    return f"{LABEL_PREFIX}.{job}"


def uv_bin_dir() -> str:
    """Directory containing the ``uv`` binary, for the agent's PATH.

    launchd agents run with a minimal environment, so ``uv`` (invoked by
    run_scheduled.py) must be reachable via an explicit PATH entry.
    """
    found = shutil.which("uv")
    if found:
        return str(Path(found).parent)
    return str(Path.home() / ".local" / "bin")


def launch_python() -> str:
    """Stable interpreter for the launchd agent's ProgramArguments[0].

    ``run_scheduled.py`` is stdlib-only, so prefer the system ``python3`` --
    always present on macOS and unaffected by a rebuilt/relocated project venv
    (the very failure mode this repo hit). Falls back to the current interpreter
    only if the system one is missing.
    """
    system_python = Path("/usr/bin/python3")
    if system_python.exists():
        return str(system_python)
    return sys.executable


def build_plist(job: str) -> dict:
    """Construct the launchd property-list dict for a job."""
    log_dir = REPO / "logs"
    path_env = f"{uv_bin_dir()}:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    return {
        "Label": label_for(job),
        "ProgramArguments": [
            launch_python(),
            str(REPO / "scripts" / "run_scheduled.py"),
            job,
        ],
        "WorkingDirectory": str(REPO),
        "StartCalendarInterval": SCHEDULES[job],
        "EnvironmentVariables": {"PATH": path_env},
        "StandardOutPath": str(log_dir / f"launchd-{job}.out.log"),
        "StandardErrorPath": str(log_dir / f"launchd-{job}.err.log"),
        "RunAtLoad": False,
        "ProcessType": "Background",
    }


def plist_path(job: str) -> Path:
    return LAUNCH_AGENTS_DIR / f"{label_for(job)}.plist"


def cmd_dry_run() -> int:
    for job in sorted(SCHEDULES):
        print(f"# ==== {label_for(job)} ({plist_path(job)}) ====")
        print(plistlib.dumps(build_plist(job)).decode())
    return 0


def cmd_install(load: bool) -> int:
    LAUNCH_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPO / "logs").mkdir(parents=True, exist_ok=True)
    for job in sorted(SCHEDULES):
        target = plist_path(job)
        with open(target, "wb") as fh:
            plistlib.dump(build_plist(job), fh)
        print(f"wrote {target}")
        if load:
            # Unload any prior version first (ignore failure -- it may not be
            # loaded yet), then load. `load` is check=True so a genuine failure
            # surfaces loudly.
            subprocess.run(
                ["launchctl", "unload", str(target)],
                check=False,
            )
            subprocess.run(
                ["launchctl", "load", str(target)],
                check=True,
            )
            print(f"loaded {label_for(job)}")
    if not load:
        print(
            "\nPlists written but NOT loaded. Re-run with `install --load` to "
            "activate, or `launchctl load <plist>` manually."
        )
    return 0


def cmd_uninstall() -> int:
    for job in sorted(SCHEDULES):
        target = plist_path(job)
        if target.exists():
            subprocess.run(
                ["launchctl", "unload", str(target)],
                check=False,
            )
            target.unlink()
            print(f"removed {target}")
        else:
            print(f"skip (absent) {target}")
    return 0


def main() -> int:
    if sys.platform != "darwin":
        print(
            "install_launchd.py only runs on macOS (launchd). Detected platform: "
            f"{sys.platform}",
            file=sys.stderr,
        )
        return 1

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_install = sub.add_parser("install", help="write plists to ~/Library/LaunchAgents")
    p_install.add_argument(
        "--load",
        action="store_true",
        help="also load the agents into launchd (starts the schedules)",
    )
    sub.add_parser("uninstall", help="unload and remove the plists")
    sub.add_parser("dry-run", help="print the generated plist XML; write nothing")

    args = parser.parse_args()

    if args.command == "install":
        return cmd_install(load=args.load)
    if args.command == "uninstall":
        return cmd_uninstall()
    if args.command == "dry-run":
        return cmd_dry_run()
    parser.error(f"unknown command {args.command!r}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
