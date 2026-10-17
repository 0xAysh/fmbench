"""`run`: sequential replay of a query file, for G1 and G7.

Each query × repetition is timed on the raw bytes; the body is parsed, its logs counted and
digested, and the counter deltas stored only after the timed region.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from typing import Any

from fmbench import cold, meta, prom, rpc, sample

# The log fields that both paths and Reth 2.7.0 return. Optional fields such as `blockTimestamp`
# stay out, so a run in `today` mode compares too.
DIGEST_FIELDS = (
    "address",
    "topics",
    "data",
    "blockNumber",
    "blockHash",
    "transactionHash",
    "transactionIndex",
    "logIndex",
    "removed",
)


def digest(logs: list[dict[str, Any]]) -> str:
    """Returns the sha256 of the logs sorted by (`blockNumber`, `logIndex`), each reduced to
    `DIGEST_FIELDS` with lowercased hex, as compact JSON with sorted keys."""
    ordered = sorted(logs, key=lambda log: (int(log["blockNumber"], 16), int(log["logIndex"], 16)))
    kept = [{field: _lower(log.get(field)) for field in DIGEST_FIELDS} for log in ordered]
    canonical = json.dumps(kept, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _lower(value: Any) -> Any:
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, list):
        return [_lower(item) for item in value]
    return value


def counter_delta(before: prom.Snapshot, after: prom.Snapshot) -> dict[str, dict[str, float]]:
    """Returns the delta of every `reth_rpc_eth_filter_*` series that only grows, keyed by name
    and then by its `path` label (`""` for a series without one)."""
    delta = prom.delta(
        prom.counters(before, prom.RPC_FILTER_PREFIX), prom.counters(after, prom.RPC_FILTER_PREFIX)
    )
    out: dict[str, dict[str, float]] = {}
    for (name, labels), value in sorted(delta.items()):
        out.setdefault(name, {})[dict(labels).get("path", "")] = value
    return out


def parse_logs(response: rpc.Response) -> tuple[list[dict[str, Any]] | None, Any]:
    """Returns the logs of a response, or `None` and its error: the transport error, the JSON-RPC
    error object, or the HTTP status with the start of a body that is not JSON-RPC."""
    if response.error is not None:
        return None, response.error
    try:
        reply = json.loads(response.body)
    except ValueError:
        return None, f"HTTP {response.status}: {response.body[:200].decode(errors='replace')}"
    if "error" in reply:
        return None, reply["error"]
    if response.status != 200 or not isinstance(reply.get("result"), list):
        return None, f"HTTP {response.status}: {response.body[:200].decode(errors='replace')}"
    return reply["result"], None


def result_line(
    query_id: str, rep: int, response: rpc.Response, counters: dict[str, dict[str, float]]
) -> dict[str, Any]:
    """Builds one `seq.jsonl` line. An error line has no log count and no digest."""
    logs, error = parse_logs(response)
    return {
        "query_id": query_id,
        "rep": rep,
        "latency_ms": response.latency_ms,
        "bytes": len(response.body),
        "logs": None if logs is None else len(logs),
        "digest": None if logs is None else digest(logs),
        "error": error,
        "counters": counters,
    }


def measure(
    client: rpc.Client, metrics_url: str | None, query_id: str, rep: int, filter_: dict[str, Any]
) -> dict[str, Any]:
    """Runs one query: scrape, send the request (timed on the raw bytes), scrape again, then
    parse, count, digest and take the counter deltas. Without `metrics_url` (mode `today`) there
    are no counters."""
    body = client.request_body("eth_getLogs", [filter_])
    before = prom.scrape(metrics_url) if metrics_url else None
    response = client.call_raw(body)
    after = prom.scrape(metrics_url) if metrics_url else None
    counters = counter_delta(before, after) if before is not None and after is not None else {}
    return result_line(query_id, rep, response, counters)


def check_window(queries: list[dict[str, Any]], snapshot: prom.Snapshot) -> None:
    """Refuses, in index mode, a query outside the indexed window: above the index head the node
    has no maps, and below the first indexed block the bloom path would serve part of it."""
    first = snapshot.value("reth_filter_maps_first_indexed_block")
    last = snapshot.value("reth_filter_maps_last_indexed_block")
    if not first or not last:
        raise SystemExit("the node reports no indexed window (reth_filter_maps_*_indexed_block)")
    outside = [
        q["id"]
        for q in queries
        if int(q["filter"]["fromBlock"], 16) < first or int(q["filter"]["toBlock"], 16) > last
    ]
    if outside:
        raise SystemExit(
            f"{len(outside)} queries leave the indexed window {int(first)}..{int(last)} (first: {outside[0]}); "
            "a query above the head has no maps, and below the first indexed block (the tail moves up as the "
            "chain grows) the bloom path serves part of it. Sample again with a larger --tail-margin"
        )


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("run", help="sequential replay: latency, counter deltas and digests (G1, G7)")
    meta.add_node_args(parser)
    meta.add_result_args(parser)
    parser.add_argument("--queries", required=True, help="a query file from sample")
    parser.add_argument("--cache", choices=meta.CACHES, default="warm", help="cold drops the page cache and restarts the node before every repetition of every query")
    parser.add_argument("--reps", type=int, default=25, help="repetitions per query (use 5 with --cache cold)")
    parser.add_argument("--limit", type=int, help="run only the first N queries of the file")
    parser.add_argument("--timeout", type=float, default=rpc.DEFAULT_TIMEOUT_S, help="per-request timeout in seconds")
    parser.add_argument("--spread", action="store_true", help="mark this run as the repeat that measures the run-to-run spread")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    header, queries = sample.read_queries(args.queries)
    queries = queries[: args.limit]
    if args.mode == "index":
        check_window(queries, prom.scrape(args.metrics))
    info = meta.collect(
        "run",
        args.mode,
        args.cache,
        args.unit,
        args.pid,
        kind=header.get("kind"),
        queries=meta.file_info(args.queries),
        queries_used=len(queries),
        reps=args.reps,
        timeout_s=args.timeout,
        spread=args.spread,
    )
    directory = meta.run_dir(args.out, info, args.run_id)
    meta.write(directory, info)
    client = rpc.Client(args.rpc, timeout_s=args.timeout)
    metrics_url = None if args.mode == "today" else args.metrics
    errors = 0
    with open(os.path.join(directory, "seq.jsonl"), "w") as out:
        # Repetition-major order: the other queries run between two repetitions of one query, so a
        # repetition does not just reread what the previous one left in the node's caches.
        for rep in range(args.reps):
            for n, query in enumerate(queries):
                if args.cache == "cold":
                    cold.restart(args.unit, args.rpc, args.metrics, args.mode == "index")
                    client.close()
                line = measure(client, metrics_url, query["id"], rep, query["filter"])
                errors += line["error"] is not None
                out.write(json.dumps(line) + "\n")
                out.flush()
                if (n + 1) % 50 == 0 or n + 1 == len(queries):
                    print(f"rep {rep + 1}/{args.reps}: {n + 1}/{len(queries)} queries, {errors} errors", file=sys.stderr)
    info["ended"] = meta.now()
    meta.write(directory, info)
    print(directory)
    return 0
