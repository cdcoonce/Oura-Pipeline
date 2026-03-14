#!/usr/bin/env python3
"""Post-edit hook: auto-format with Prettier and lint JS/CSS/Python files."""

import json
import os
import shutil
import subprocess
import sys

data = json.load(sys.stdin)
file_path = data.get("tool_input", {}).get("file_path", "")

if not file_path:
    sys.exit(0)

is_windows = os.name == "nt"
actions = []


def run(cmd, label):
    """Run a command silently, ignoring failures."""
    try:
        result = subprocess.run(cmd, shell=is_windows, capture_output=True)
        if result.returncode == 0:
            actions.append(label)
    except FileNotFoundError:
        pass


# Prettier on supported file types
if file_path.endswith((".html", ".css", ".js", ".md", ".json")):
    run(["npx", "prettier", "--write", file_path], "prettier")

# ESLint on JS files
if file_path.endswith(".js"):
    run(["npx", "eslint", "--fix", file_path], "eslint")

# Stylelint on CSS files
if file_path.endswith(".css"):
    run(["npx", "stylelint", "--fix", file_path], "stylelint")

# Ruff on Python files
if file_path.endswith(".py") and shutil.which("ruff"):
    run(["ruff", "check", "--fix", file_path], "ruff-check")
    run(["ruff", "format", file_path], "ruff-format")

# Report what ran (shown in Claude's context via stderr)
if actions:
    print(f"Hook ran: {', '.join(actions)} on {os.path.basename(file_path)}", file=sys.stderr)
