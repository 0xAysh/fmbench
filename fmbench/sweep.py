"""`sweep`: one filter over the window in fixed chunks, one request after another, as an indexer
backfill does (roadmap §1.8). The headline number: total time, blocks/s, requests/s.

Warm only: a whole-window sweep reads far more than RAM, so it is disk-bound in any cache state.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from fmbench import meta, prom, rpc, run, sample


def chunks(first: int, last: int, size: int) -> list[tuple[int, int]]:
    """Returns `first..=last` as consecutive ranges of `size` blocks; the last one may be shorter."""
    return [(start, min(start + size - 1, last)) for start in range(first, last + 1, size)]


def summarize(lines: list[dict[str, Any]], filter_id: str, chunk: int, run_number: int, planned: int) -> dict[str, Any]:
    """Returns the summary line of one sweep run. Time is the sum of the request latencies (the
    metric scrapes between requests are not part of it). A run that an error ended is incomplete;
    `reached` is the last block of its last answered chunk."""
    answered = [line for line in lines if line["error"] is None]
    error = next((line["error"] for line in lines if line["error"] is not None), None)
    total_ms = sum(line["latency_ms"] for line in lines)
    blocks = sum(line["chunk_to"] - line["chunk_from"] + 1 for line in answered)
    seconds = total_ms / 1000
    counters: dict[str, dict[str, float]] = {}
    for line in lines:
        run.add_counters(counters, line["counters"])
    return {
        "summary": True,
        "filter_id": filter_id,
        "chunk": chunk,
        "run": run_number,
        "requests": len(lines),
        "planned_requests": planned,
        "blocks": blocks,
        "total_ms": total_ms,
        "blocks_per_s": blocks / seconds if seconds else None,
        "requests_per_s": len(lines) / seconds if seconds else None,
        "logs": sum(line["logs"] for line in answered),
        "counters": counters,
        "completed": error is None and len(lines) == planned,
        "reached": answered[-1]["chunk_to"] if answered else None,
        "error": error,
    }


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("sweep", help="one filter over the window in fixed chunks, one request after another")
    meta.add_node_args(parser)
    meta.add_result_args(parser)
    parser.add_argument("--queries", required=True, help="the query file that holds --query")
    parser.add_argument("--query", required=True, help="the query id whose address and topics are swept (its range is ignored)")
    parser.add_argument("--chunk", type=int, required=True, help="blocks per request")
    parser.add_argument("--runs", type=int, default=2)
    parser.add_argument("--from", dest="first", type=int, help="first block (default: the query file's window)")
    parser.add_argument("--to", dest="last", type=int, help="last block (default: the query file's window)")
    parser.add_argument("--timeout", type=float, default=rpc.DEFAULT_TIMEOUT_S, help="per-request timeout in seconds")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    header, queries = sample.read_queries(args.queries)
    query = next((q for q in queries if q["id"] == args.query), None)
    if query is None:
        raise SystemExit(f"{args.queries} has no query {args.query!r}")
    selector = {k: v for k, v in query["filter"].items() if k not in ("fromBlock", "toBlock")}
    first = header["window"][0] if args.first is None else args.first
    last = header["window"][1] if args.last is None else args.last
    planned = chunks(first, last, args.chunk)
    if args.mode == "index":
        run.check_window([("the sweep range", first, last)], prom.scrape(args.metrics))
    info = meta.collect(
        "sweep",
        args.mode,
        "warm",
        args.unit,
        kind="sweep",
        queries=meta.file_info(args.queries),
        filter_id=args.query,
        chunk=args.chunk,
        runs=args.runs,
        range=[first, last],
        timeout_s=args.timeout,
    )
    directory = meta.run_dir(args.out, info, f"{args.query}-c{args.chunk}")
    meta.write(directory, info)
    client = rpc.Client(args.rpc, timeout_s=args.timeout)
    with open(os.path.join(directory, "sweep.jsonl"), "w") as out:
        for run_number in range(1, args.runs + 1):
            lines = []
            for n, (a, b) in enumerate(planned, 1):
                measured = run.measure(client, args.metrics, {**selector, "fromBlock": hex(a), "toBlock": hex(b)})
                line = {"run": run_number, "chunk_from": a, "chunk_to": b, **measured}
                lines.append(line)
                out.write(json.dumps(line) + "\n")
                out.flush()
                if line["error"] is not None:
                    break
                if n % max(1, len(planned) // 10) == 0:
                    print(f"run {run_number}: {n}/{len(planned)} chunks", file=sys.stderr)
            summary = summarize(lines, args.query, args.chunk, run_number, len(planned))
            out.write(json.dumps(summary) + "\n")
            out.flush()
            state = "complete" if summary["completed"] else f"INCOMPLETE at block {summary['reached']}: {summary['error']}"
            print(f"run {run_number}: {summary['requests']} requests, {summary['total_ms'] / 1000:.1f} s, {summary['blocks_per_s'] or 0:.0f} blocks/s, {state}")
    info["ended"] = meta.now()
    meta.write(directory, info)
    print(directory)
    return 0
