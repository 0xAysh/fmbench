"""`watch`: the index build rate, the store size, the index lag (G3) and the node's per-block
execution time (G4) over time.

Block times come from the node's JSON file log (`reth node --log.file.format json`): every
`Block added to canonical chain` line carries the block's `elapsed`.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import re
import sys
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from fmbench import meta, prom, rpc, stats

BLOCK_MESSAGE = "Block added to canonical chain"

NEW_PAYLOAD = "reth_consensus_engine_beacon_new_payload_latency"
# off: the index off (the catch-up's G4 reference); catchup: the index building; load: during a G8
# load, in bloom mode as the reference of the index-mode one.
PHASES = ("off", "catchup", "load")
# The timeout of the finalized-block request of each sample.
RPC_TIMEOUT_S = 30
# The node's `reth_filter_maps_*` series: the index columns of `watch.csv`. The set is fixed, so a
# first sample taken before the node registers the series still gets every column.
SERIES = tuple(
    prom.FILTER_MAPS_PREFIX + name
    for name in (
        "blocks_rendered", "failures", "first_indexed_block", "last_indexed_block", "maps_written", "resets", "running",
        "step_duration_seconds_count", "step_duration_seconds_sum", "store_target",
        "write_duration_seconds_count", "write_duration_seconds_sum",
    )
)

_DURATION = re.compile(r"(\d+(?:\.\d+)?)(ns|µs|μs|us|ms|s)")
_NS_PER_UNIT = {"ns": 1, "µs": 1_000, "μs": 1_000, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


def parse_duration_ns(text: str) -> int:
    """Parses a Rust `Duration` debug string (`45.1ms`, `850µs`, `1.5s`) into nanoseconds."""
    match = _DURATION.fullmatch(text)
    if match is None:
        raise ValueError(f"not a Rust Duration: {text!r}")
    return int(Decimal(match.group(1)) * _NS_PER_UNIT[match.group(2)])


def block_line(text: str) -> tuple[int, int] | None:
    """Returns the number and execution time (ns) of a canonical-block log line, or `None` for any
    other line. Accepts both the nested (`fields`) and the flattened JSON layout."""
    try:
        line = json.loads(text)
    except ValueError:
        return None
    fields: Any = line.get("fields", line) if isinstance(line, dict) else None
    if not isinstance(fields, dict) or fields.get("message") != BLOCK_MESSAGE:
        return None
    return int(fields["number"]), parse_duration_ns(fields["elapsed"])


@dataclass
class Sample:
    """One interval's readings."""

    elapsed_s: float
    # The `reth_filter_maps_*` series, without summary quantiles; empty while the index is off.
    series: dict[str, float]
    finalized: int | None
    store_bytes: int | None
    # The execution times of the blocks added since the previous sample.
    block_ns: list[int]
    # `_sum` (seconds) and `_count` of the node's `newPayload` latency.
    new_payload: tuple[float, float] | None


def derive(prev: Sample | None, cur: Sample) -> dict[str, float | None]:
    """Returns the derived columns of `cur`. Rates need `prev`; a series the node lacks gives
    `None`. `lag_maps` divides the lag behind the finalized block by the mean blocks per map since
    the node started."""
    fm = prom.FILTER_MAPS_PREFIX
    series = cur.series.get
    first, last = series(fm + "first_indexed_block"), series(fm + "last_indexed_block")
    target, rendered, maps = series(fm + "store_target"), series(fm + "blocks_rendered"), series(fm + "maps_written")
    indexed = last - first + 1 if first is not None and last else None
    blocks_per_map = rendered / maps if rendered and maps else None
    block_ms = [ns / 1e6 for ns in cur.block_ns]
    return {
        "blocks_per_s": _rate(prev, cur, fm + "blocks_rendered"),
        "maps_per_s": _rate(prev, cur, fm + "maps_written"),
        "step_mean_s": _mean(prev, cur, fm + "step_duration_seconds"),
        "write_mean_s": _mean(prev, cur, fm + "write_duration_seconds"),
        "lag_blocks": target - last if target is not None and last else None,
        "lag_maps": (cur.finalized - last) / blocks_per_map if cur.finalized and last and blocks_per_map else None,
        "bytes_per_block": cur.store_bytes / indexed if cur.store_bytes is not None and indexed else None,
        "block_count": len(block_ms),
        "block_p50_ms": stats.percentile(block_ms, 0.5),
        "block_p99_ms": stats.percentile(block_ms, 0.99),
        "new_payload_mean_ms": _new_payload_mean_ms(prev, cur),
    }


def _rate(prev: Sample | None, cur: Sample, name: str) -> float | None:
    if prev is None or name not in cur.series or name not in prev.series:
        return None
    return (cur.series[name] - prev.series[name]) / (cur.elapsed_s - prev.elapsed_s)


def _mean(prev: Sample | None, cur: Sample, family: str) -> float | None:
    """Returns the mean of a summary's observations between two samples."""
    if prev is None:
        return None
    try:
        total = cur.series[family + "_sum"] - prev.series[family + "_sum"]
        count = cur.series[family + "_count"] - prev.series[family + "_count"]
    except KeyError:
        return None
    return total / count if count else None


def _new_payload_mean_ms(prev: Sample | None, cur: Sample) -> float | None:
    if prev is None or prev.new_payload is None or cur.new_payload is None:
        return None
    count = cur.new_payload[1] - prev.new_payload[1]
    return (cur.new_payload[0] - prev.new_payload[0]) / count * 1000 if count else None


def rows(samples: Iterable[Sample | None]) -> Iterator[list[Any]]:
    """Yields the `watch.csv` header, then one row per sample. `None` is an interval in which the
    node did not answer (stopped or still starting): it gets no row, and the next row gets no
    rates, because a restart resets the node's counters."""
    header = True
    prev: Sample | None = None
    for cur in samples:
        if cur is None:
            prev = None
            continue
        derived = derive(prev, cur)
        if header:
            header = False
            yield ["time", "elapsed_s", "finalized", "store_bytes", "new_payload_sum", "new_payload_count", *SERIES, *derived]
        payload = cur.new_payload or (None, None)
        yield [meta.now(), round(cur.elapsed_s, 3), cur.finalized, cur.store_bytes, *payload, *(cur.series.get(c) for c in SERIES), *derived.values()]
        prev = cur


class LogTail:
    """Reads the lines appended to a log file since the last read. It starts at the end of the
    file and reopens it from the start after a rotation (a new inode or a shorter file)."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._inode = os.stat(path).st_ino
        self._offset = os.path.getsize(path)
        self._partial = b""

    def read(self) -> list[str]:
        """Returns the whole lines appended since the last read; a partial last line waits for
        the next read."""
        status = os.stat(self.path)
        if status.st_ino != self._inode or status.st_size < self._offset:
            self._inode, self._offset, self._partial = status.st_ino, 0, b""
        with open(self.path, "rb") as file:
            file.seek(self._offset)
            data = self._partial + file.read()
            self._offset = file.tell()
        *lines, self._partial = data.split(b"\n")
        return [line.decode(errors="replace") for line in lines]


def dir_bytes(path: str) -> int | None:
    """Returns the apparent size of the files under `path` (as `du -sb`), following `path` itself
    if it is a symlink, or `None` if it does not exist."""
    if not os.path.exists(path):
        return None
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except FileNotFoundError:
                pass  # RocksDB deleted a file during the walk.
    return total


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("watch", help="index build rate, store size, index lag and block times over time")
    meta.add_node_args(parser)
    meta.add_result_args(parser)
    parser.add_argument("--phase", required=True, choices=PHASES, help="off: index off (the catch-up's G4 reference); catchup: index building; load: during a G8 load, in both modes")
    parser.add_argument("--log-file", required=True, help="the node's JSON log file (reth.log under --log.file.directory/<chain>)")
    parser.add_argument("--datadir", help="the node's datadir (default: its --datadir)")
    parser.add_argument("--interval", type=float, default=10, help="seconds between samples")
    parser.add_argument("--duration", type=float, help="seconds to watch (default: until Ctrl-C)")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    if args.phase == "off" and args.mode == "index":
        raise SystemExit("phase off is the index-off reference: use --mode bloom")
    info = meta.collect("watch", args.mode, "warm", args.unit, phase=args.phase, interval_s=args.interval, log_file=args.log_file)
    if meta.arg(info["cmdline"], "--log.file.format") != "json":
        raise SystemExit("the node does not log JSON to its file: start it with --log.file.format json")
    datadir = args.datadir or info["datadir"]
    if datadir is None:
        raise SystemExit("the node has no --datadir on its command line: pass --datadir")
    directory = meta.run_dir(args.out, info, args.phase)
    meta.write(directory, info)
    tail = LogTail(args.log_file)
    client = rpc.Client(args.rpc, timeout_s=RPC_TIMEOUT_S)
    with open(os.path.join(directory, "watch.csv"), "w", newline="") as watch_file, open(
        os.path.join(directory, "blocks.csv"), "w", newline=""
    ) as blocks_file:
        blocks = csv.writer(blocks_file)
        blocks.writerow(["time", "number", "elapsed_ms"])
        writer = csv.writer(watch_file)
        try:
            for row in rows(_poll(args, datadir, client, tail, blocks)):
                writer.writerow(row)
                watch_file.flush()
                blocks_file.flush()
        except KeyboardInterrupt:
            pass
    info["ended"] = meta.now()
    meta.write(directory, info)
    print(directory)
    return 0


def _poll(args: argparse.Namespace, datadir: str, client: rpc.Client, tail: LogTail, blocks: Any) -> Iterator[Sample | None]:
    """Yields a sample every `--interval` seconds, for `--duration` seconds or until Ctrl-C, and
    `None` for an interval in which `/metrics` does not answer."""
    started = time.monotonic()
    answered = True
    while args.duration is None or time.monotonic() - started < args.duration:
        elapsed_s = time.monotonic() - started
        try:
            snapshot = prom.scrape(args.metrics)
        except (OSError, http.client.HTTPException) as error:
            # The node is stopped or still starting. One line when it stops answering, so a wrong
            # --metrics URL shows at once, and one when it answers again.
            if answered:
                print(f"{meta.now()} no answer from {args.metrics}, retrying: {error}", file=sys.stderr)
            answered = False
            yield None
        else:
            if not answered:
                print(f"{meta.now()} {args.metrics} answers again", file=sys.stderr)
            answered = True
            yield _sample(elapsed_s, snapshot, client, datadir, tail, blocks)
        time.sleep(max(0.0, args.interval - (time.monotonic() - started - elapsed_s)))


def _sample(elapsed_s: float, snapshot: prom.Snapshot, client: rpc.Client, datadir: str, tail: LogTail, blocks: Any) -> Sample:
    series = {name: value for (name, labels), value in snapshot.items() if name.startswith(prom.FILTER_MAPS_PREFIX) and not labels}
    payload_sum, payload_count = snapshot.value(NEW_PAYLOAD + "_sum"), snapshot.value(NEW_PAYLOAD + "_count")
    try:
        finalized = int(client.call("eth_getBlockByNumber", ["finalized", False])["number"], 16)
    except (rpc.RpcError, TypeError):
        finalized = None
    block_ns = []
    for line in tail.read():
        parsed = block_line(line)
        if parsed is not None:
            block_ns.append(parsed[1])
            blocks.writerow([meta.now(), parsed[0], parsed[1] / 1e6])
    return Sample(
        elapsed_s=elapsed_s,
        series=series,
        finalized=finalized,
        store_bytes=dir_bytes(os.path.join(datadir, "filter-maps")),
        block_ns=block_ns,
        new_payload=(payload_sum, payload_count) if payload_sum is not None and payload_count is not None else None,
    )
