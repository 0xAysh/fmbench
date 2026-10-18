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
# Persistences since the start: the engine writes its in-memory blocks in one batch once more
# than 50 wait, which a restarted node reaches during its catch-up.
PERSISTED = "reth_consensus_engine_beacon_persistence_duration_count"
# The indexer series of the wait: steps finished since the start, and failed steps.
STEPS = prom.FILTER_MAPS_PREFIX + "step_duration_seconds_count"
FAILURES = prom.FILTER_MAPS_PREFIX + "failures"


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
    latest block is at most `AT_TIP_S` old and it has persisted blocks since the start and, in
    index mode, the indexer has finished a step since the start, none has failed, and
    `reth_filter_maps_last_indexed_block` is back at its value before the stop. A restarted node
    unwinds and executes its last blocks again, then the blocks of its downtime, and writes them
    in one batch; a query sent earlier would compete with that work. The indexer steps only after
    that write, so every mode waits for it.

    The first indexer step renders the map group in progress again (32 maps), since a restart
    resumes at the group's first map; `sample` keeps the queries out of those blocks. A step
    renders at most 1,000 blocks, so after a long downtime the indexer still catches up when the
    wait ends. Later steps render only the blocks finalized since, as every epoch of a warm run
    does, so `cold` does not wait for finality to move."""
    ready = f"the latest block is at most {AT_TIP_S} s old, the node has persisted blocks since the start" + (
        " and the FilterMaps indexer has finished a step since the start, with its head back at its value before the stop"
        if index_mode
        else ""
    )
    if dry_run:
        for step in steps(unit):
            print(shlex.join(step))
        print(f"wait up to {timeout_s:.0f} s until {ready}")
        return
    head = None
    if index_mode:
        try:
            window = prom.indexed_window(prom.scrape(metrics_url, timeout_s=PROBE_TIMEOUT_S))
        except OSError as error:
            raise ColdError(f"cannot read {metrics_url} before the stop: {error}") from error
        if window is None:
            raise ColdError("the node reports no FilterMaps index before the stop")
        head = window[1]
    for step in steps(unit):
        result = subprocess.run(step, capture_output=True, text=True)
        if result.returncode != 0:
            raise ColdError(f"`{shlex.join(step)}` failed ({result.returncode}): {result.stderr.strip()}")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _caught_up(rpc_url, metrics_url, head):
            return
        time.sleep(1)
    raise ColdError(f"the node was not ready after {timeout_s:.0f} s: waited until {ready}")


def _caught_up(rpc_url: str, metrics_url: str, head: int | None) -> bool:
    """Whether the latest block is at most `AT_TIP_S` old, the node has persisted blocks since the
    start and, given the index head before the stop (`None` outside index mode), the indexer has
    finished a step since the start and its head is back at `head`. A failed step raises
    `ColdError`: the counter never goes back to 0."""
    try:
        latest = rpc.Client(rpc_url, timeout_s=PROBE_TIMEOUT_S).call("eth_getBlockByNumber", ["latest", False])
        if time.time() - int(latest["timestamp"], 16) > AT_TIP_S:
            return False
        snapshot = prom.scrape(metrics_url, timeout_s=PROBE_TIMEOUT_S)
    except (rpc.RpcError, OSError):
        return False
    if not snapshot.value(PERSISTED):
        return False
    if head is None:
        return True
    if failures := snapshot.value(FAILURES):
        raise ColdError(f"the FilterMaps indexer has failed since the start ({FAILURES} {failures:.0f}); see the node log")
    window = prom.indexed_window(snapshot)
    return bool(snapshot.value(STEPS)) and window is not None and window[1] >= head


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
