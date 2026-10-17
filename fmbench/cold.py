"""`cold`: stop the node, drop the page cache, start it, and wait until it serves queries.

The node is stopped first so that its shutdown writes are flushed and dropped too. Every step
needs root: as another user the commands run through `sudo -n`, which fails instead of prompting.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import time
from collections.abc import Callable
from typing import Any

from fmbench import meta, prom, rpc

DEFAULT_TIMEOUT_S = 900


class ColdError(Exception):
    """A step failed, or the node was not ready in time."""


def steps(unit: str) -> list[list[str]]:
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
    log: Callable[[str], None] = print,
) -> None:
    """Drops the page cache around a node restart, then waits until `eth_blockNumber` answers and,
    in index mode, `reth_filter_maps_last_indexed_block` is set (the indexer sets it at startup
    from its store)."""
    ready = "eth_blockNumber answers" + (" and reth_filter_maps_last_indexed_block > 0" if index_mode else "")
    for step in steps(unit):
        if dry_run:
            log(shlex.join(step))
            continue
        result = subprocess.run(step, capture_output=True, text=True)
        if result.returncode != 0:
            raise ColdError(f"`{shlex.join(step)}` failed ({result.returncode}): {result.stderr.strip()}")
    if dry_run:
        log(f"wait up to {timeout_s:.0f} s until {ready}")
        return
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _ready(rpc_url, metrics_url, index_mode):
            return
        time.sleep(1)
    raise ColdError(f"the node was not ready after {timeout_s:.0f} s: waited until {ready}")


def _ready(rpc_url: str, metrics_url: str, index_mode: bool) -> bool:
    try:
        rpc.Client(rpc_url, timeout_s=5).call("eth_blockNumber", [])
        if index_mode:
            return prom.indexed_window(prom.scrape(metrics_url, timeout_s=5)) is not None
        return True
    except (rpc.RpcError, OSError):
        return False


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("cold", help="drop the page cache around a node restart and wait until it is ready")
    meta.add_node_args(parser)
    parser.add_argument("--mode", required=True, choices=meta.MODES, help="index waits for the index gauges too")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds to wait until the node is ready")
    parser.add_argument("--dry-run", action="store_true", help="print the steps instead of running them")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    try:
        restart(args.unit, args.rpc, args.metrics, args.mode == "index", args.timeout, args.dry_run)
    except ColdError as error:
        print(f"cold: {error}")
        return 1
    return 0
