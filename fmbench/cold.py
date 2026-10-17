"""`cold`: stop the node, drop the page cache, start it, and wait until it has caught up.

The node is stopped first so that its shutdown writes are flushed and dropped too. Every step
needs root: as another user the commands run through `sudo -n`, which fails instead of prompting.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import time
from typing import Any

from fmbench import meta, prom, rpc

DEFAULT_TIMEOUT_S = 900
# The timeout of one readiness probe.
PROBE_TIMEOUT_S = 5
# The node has caught up when its latest block is at most this old: two mainnet slots, so one
# missed slot does not hold the wait.
AT_TIP_S = 24


class ColdError(Exception):
    """A step failed, or the node was not ready in time."""


def steps(unit: str) -> list[list[str]]:
    """Returns the commands of a cold restart: stop the unit, flush, drop the page cache, start."""
    prefix = [] if os.geteuid() == 0 else ["sudo", "-n"]
    return [
        prefix + ["systemctl", "stop", unit],
        prefix + ["sync"],
        prefix + ["sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
        prefix + ["systemctl", "start", unit],
    ]


def restart(
    unit: str,
    rpc_url: str,
    metrics_url: str,
    index_mode: bool,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    dry_run: bool = False,
) -> None:
    """Drops the page cache around a node restart, then waits until the node has caught up: its
    latest block is at most `AT_TIP_S` old and, in index mode, `reth_filter_maps_last_indexed_block`
    is back at its value before the stop. A restarted node unwinds and executes its last blocks
    again, and then the blocks of its downtime; a query sent earlier would compete with that work."""
    ready = f"the latest block is at most {AT_TIP_S} s old" + (
        " and reth_filter_maps_last_indexed_block is back at its value before the stop" if index_mode else ""
    )
    if dry_run:
        for step in steps(unit):
            print(shlex.join(step))
        print(f"wait up to {timeout_s:.0f} s until {ready}")
        return
    indexed = 0
    if index_mode:
        window = prom.indexed_window(prom.scrape(metrics_url, timeout_s=PROBE_TIMEOUT_S))
        if window is None:
            raise ColdError("the node reports no indexed window before the stop")
        indexed = window[1]
    for step in steps(unit):
        result = subprocess.run(step, capture_output=True, text=True)
        if result.returncode != 0:
            raise ColdError(f"`{shlex.join(step)}` failed ({result.returncode}): {result.stderr.strip()}")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _caught_up(rpc_url, metrics_url, indexed):
            return
        time.sleep(1)
    raise ColdError(f"the node was not ready after {timeout_s:.0f} s: waited until {ready}")


def _caught_up(rpc_url: str, metrics_url: str, indexed: int) -> bool:
    try:
        latest = rpc.Client(rpc_url, timeout_s=PROBE_TIMEOUT_S).call("eth_getBlockByNumber", ["latest", False])
        if time.time() - int(latest["timestamp"], 16) > AT_TIP_S:
            return False
        if not indexed:
            return True
        window = prom.indexed_window(prom.scrape(metrics_url, timeout_s=PROBE_TIMEOUT_S))
    except (rpc.RpcError, OSError):
        return False
    return window is not None and window[1] >= indexed


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("cold", help="drop the page cache around a node restart and wait until it is ready")
    meta.add_node_args(parser)
    parser.add_argument("--mode", required=True, choices=meta.MODES, help="index waits for the index gauges too")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds to wait until the node is ready")
    parser.add_argument("--dry-run", action="store_true", help="print the steps instead of running them")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    restart(args.unit, args.rpc, args.metrics, args.mode == "index", args.timeout, args.dry_run)
    return 0
