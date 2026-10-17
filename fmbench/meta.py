"""`meta.json` and the result files: what a result was measured on. A result without it is invalid.

It reads the running node from `/proc/<pid>`, so the kit runs on the node's machine (Linux).
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import platform
import re
import subprocess
from typing import Any

MODES = ("index", "bloom", "today")
CACHES = ("warm", "cold")

DEFAULT_RPC = "http://127.0.0.1:8545"
DEFAULT_METRICS = "http://127.0.0.1:9001/"
DEFAULT_UNIT = "reth"


class MetaError(Exception):
    """The node's identity could not be read, or does not match the requested mode."""


def add_node_args(parser: argparse.ArgumentParser) -> None:
    """Adds the options that locate the node, shared by every command that talks to it."""
    parser.add_argument("--rpc", default=DEFAULT_RPC, help="the node's HTTP JSON-RPC URL")
    parser.add_argument("--metrics", default=DEFAULT_METRICS, help="the node's --metrics URL")
    parser.add_argument("--unit", default=DEFAULT_UNIT, help="the node's systemd unit")


def add_result_args(parser: argparse.ArgumentParser) -> None:
    """Adds the options of every command that writes a result directory."""
    parser.add_argument("--mode", required=True, choices=MODES, help="index (--filter-maps), bloom (same binary without it), today (Reth 2.7.0)")
    parser.add_argument("--out", default="results", help="results root: writes <out>/<commit>/<run-id>/")


def now() -> str:
    """Returns the UTC time, to the second, in ISO 8601."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def node_pid(unit: str) -> int:
    """Returns the main process of the systemd `unit`."""
    try:
        out = subprocess.run(
            ["systemctl", "show", "-p", "MainPID", "--value", unit],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise MetaError(f"cannot read the main process of unit {unit!r} ({error})") from error
    if not out.isdigit() or out == "0":
        raise MetaError(f"unit {unit!r} has no running process; start it")
    return int(out)


def node(unit: str) -> dict[str, Any]:
    """Returns the node binary (path, sha256), its commit and version, and its command line."""
    pid = node_pid(unit)
    exe = f"/proc/{pid}/exe"
    try:
        path = os.readlink(exe)
        with open(exe, "rb") as binary:
            sha256 = _sha256(binary)
        with open(f"/proc/{pid}/cmdline", "rb") as file:
            cmdline = [arg.decode() for arg in file.read().split(b"\0") if arg]
        # `/proc/<pid>/exe` still runs the started binary after a deploy swaps the symlink.
        version = subprocess.run([exe, "--version"], capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise MetaError(f"cannot read node process {pid} ({error}); the kit runs on the node's machine") from error
    commit = re.search(r"Commit SHA: ([0-9a-f]+)", version)
    if commit is None:
        raise MetaError(f"no commit in `{path} --version`:\n{version}")
    return {
        "pid": pid,
        "binary": {"path": path, "sha256": sha256},
        "commit": commit.group(1),
        "version": version.strip(),
        "cmdline": cmdline,
    }


def check_mode(mode: str, cmdline: list[str]) -> None:
    """Refuses a run whose mode does not match the node's flags: `index` needs `--filter-maps`,
    the other modes need it off."""
    indexed = any(arg == "--filter-maps" or arg.startswith("--filter-maps=") for arg in cmdline)
    if indexed != (mode == "index"):
        state = "with" if indexed else "without"
        raise MetaError(f"mode {mode!r}, but the node runs {state} --filter-maps: {' '.join(cmdline)}")


def collect(command: str, mode: str, cache: str, unit: str, **extra: Any) -> dict[str, Any]:
    """Returns the `meta.json` of a run that starts now."""
    info = node(unit)
    check_mode(mode, info["cmdline"])
    datadir = arg(info["cmdline"], "--datadir")
    return {
        "command": command,
        "mode": mode,
        "cache": cache,
        **info,
        "datadir": datadir,
        "filesystem": _run(["findmnt", "-n", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "-T", datadir]) if datadir else None,
        "uname": _run(["uname", "-a"]),
        "cpu": _cpu(),
        "ram_bytes": _ram_bytes(),
        "python": platform.python_version(),
        "started": now(),
        "ended": None,
        **extra,
    }


def file_info(path: str) -> dict[str, str]:
    """Returns the name and sha256 of an input file."""
    with open(path, "rb") as file:
        return {"file": os.path.basename(path), "sha256": _sha256(file)}


def run_dir(out: str, meta: dict[str, Any], label: str | None = None) -> str:
    """Creates and returns `<out>/<commit>/<run-id>/`, the commit shortened to 10 characters and
    the run id made of the command, mode, cache state, `label` and start time."""
    stamp = meta["started"].replace("-", "").replace(":", "")
    run_id = "-".join(part for part in (meta["command"], meta["mode"], meta["cache"], label, stamp) if part)
    path = os.path.join(out, meta["commit"][:10], run_id)
    os.makedirs(path, exist_ok=False)
    return path


def write(directory: str, meta: dict[str, Any]) -> None:
    """Writes `meta.json` into a run directory, replacing the one written at the start."""
    with open(os.path.join(directory, "meta.json"), "w") as file:
        json.dump(meta, file, indent=2)
        file.write("\n")


def read_jsonl(path: str) -> list[dict[str, Any]]:
    """Reads a JSONL file (a query file or a result), skipping blank lines."""
    with open(path) as file:
        return [json.loads(line) for line in file if line.strip()]


def arg(cmdline: list[str], flag: str) -> str | None:
    """Returns the value of `flag` (`--flag value` or `--flag=value`) in a command line."""
    for i, item in enumerate(cmdline):
        if item == flag and i + 1 < len(cmdline):
            return cmdline[i + 1]
        if item.startswith(flag + "="):
            return item[len(flag) + 1 :]
    return None


def _run(command: list[str]) -> str | None:
    try:
        return subprocess.run(command, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _cpu() -> dict[str, Any]:
    models = []
    try:
        with open("/proc/cpuinfo") as file:
            models = [line.split(":", 1)[1].strip() for line in file if line.startswith("model name")]
    except OSError:
        pass
    return {"model": models[0] if models else platform.processor(), "threads": len(models) or os.cpu_count()}


def _ram_bytes() -> int | None:
    try:
        with open("/proc/meminfo") as file:
            for line in file:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def _sha256(file: Any) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: file.read(1 << 20), b""):
        digest.update(chunk)
    return digest.hexdigest()
