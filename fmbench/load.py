"""`load`: concurrent clients on a weighted query mix for a fixed time (G8, and G4 under load).

One process per client, each with its own connection. The clients never parse a body, so they are
not the bottleneck; each records its own CPU use and the run is flagged above 80 % of a core.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import random
import time
from collections import Counter
from typing import Any

from fmbench import meta, prom, rpc, run, sample, stats

# The heavy-user mix (weights per class).
HEAVY = {"Q1": 20, "Q2": 15, "Q8": 15, "Q9": 20, "Q3": 10, "Q5": 10, "Q6": 5, "Q6d": 5}
# A query wider than this is never drawn (roadmap §1.8): one whole-window bloom query can outlast
# a 120-second run and makes the bloom QPS meaningless.
MAX_WIDTH = 100_000
# A client above this share of one core may be the bottleneck: the run is flagged.
CPU_FLAG = 0.8
# Every client starts drawing at the same time, this long after the clients are launched, so that
# all of them have started first.
START_DELAY_S = 2.0
START_DELAY_PER_CLIENT_S = 0.05


def blocks(query: dict[str, Any]) -> int:
    """Returns the block count of a query's range."""
    return int(query["filter"]["toBlock"], 16) - int(query["filter"]["fromBlock"], 16) + 1


class Mix:
    """Draws queries: a class by weight, then one of its queries uniformly. Classes without a
    query of at most `MAX_WIDTH` blocks are left out and listed in `missing`."""

    def __init__(self, queries: list[dict[str, Any]], weights: dict[str, float]) -> None:
        by_class: dict[str, list[dict[str, Any]]] = {}
        for query in queries:
            if query["class"] in weights and blocks(query) <= MAX_WIDTH:
                by_class.setdefault(query["class"], []).append(query)
        self.classes = [cls for cls in weights if cls in by_class]
        self.weights = [weights[cls] for cls in self.classes]
        self.missing = [cls for cls in weights if cls not in by_class]
        self.queries = by_class

    def draw(self, rng: random.Random) -> dict[str, Any]:
        """Draws one query: a class by weight, then one of its queries."""
        cls = rng.choices(self.classes, self.weights)[0]
        return rng.choice(self.queries[cls])


def summarize(
    records: list[dict[str, Any]],
    clients: int,
    duration_s: float,
    warmup_s: float,
    cpu: list[float],
    missing: list[str],
) -> dict[str, Any]:
    """Returns the summary line of one client count. `t_start` is seconds from the window start
    (negative during the warmup).

    QPS counts the `ok` answers that arrive inside the window, so requests in flight at either
    edge do not inflate it. Latency percentiles cover the `ok` requests that started inside it.
    """
    started = [r for r in records if 0 <= r["t_start"] < duration_s]
    ok = [r for r in started if r["status"] == "ok"]
    answered = [
        r for r in records if r["status"] == "ok" and 0 <= r["t_start"] + r["latency_ms"] / 1000 <= duration_s
    ]
    latency: dict[str, dict[str, float | None]] = {}
    for cls in ["all"] + sorted({r["class"] for r in ok}):
        values = [r["latency_ms"] for r in ok if cls in ("all", r["class"])]
        latency[cls] = {f"p{round(q * 100)}": stats.percentile(values, q) for q in (0.5, 0.95, 0.99)}
    return {
        "summary": True,
        "clients": clients,
        "duration_s": duration_s,
        "warmup_s": warmup_s,
        "requests": len(started),
        "answered_in_window": len(answered),
        "qps": len(answered) / duration_s,
        "statuses": dict(Counter(r["status"] for r in started)),
        "latency_ms": latency,
        "client_cpu_max": max(cpu),
        "client_cpu_flag": max(cpu) > CPU_FLAG,
        "missing_classes": missing,
    }


def status(response: rpc.Response) -> str:
    """Classifies a response without parsing it: a JSON-RPC error object starts within the first
    bytes (`{"jsonrpc":"2.0","id":N,"error":`), before any result."""
    if response.error == rpc.TIMEOUT:
        return "timeout"
    if response.error is not None:
        return "connection"
    if response.status != 200:
        return f"http_{response.status}"
    return "rpc_error" if b'"error"' in response.body[:64] else "ok"


def client_loop(
    client: int,
    rpc_url: str,
    timeout_s: float,
    queries: list[dict[str, Any]],
    weights: dict[str, float],
    seed: str,
    start_at: float,
    warmup_s: float,
    duration_s: float,
) -> tuple[list[dict[str, Any]], float]:
    """One client process: draws and sends queries back to back from `start_at` until the warmup
    and the window have passed. Returns every request and the client's CPU share of one core over
    the window."""
    mix = Mix(queries, weights)
    rng = random.Random(seed)
    connection = rpc.Client(rpc_url, timeout_s=timeout_s)
    bodies = {q["id"]: connection.request_body("eth_getLogs", [q["filter"]]) for q in queries}
    window_start = start_at + warmup_s
    end = window_start + duration_s
    time.sleep(max(0.0, start_at - time.time()))
    records = []
    cpu_start: tuple[float, float] | None = None
    while (now := time.time()) < end:
        if cpu_start is None and now >= window_start:
            cpu_start = (time.process_time(), time.perf_counter())
        query = mix.draw(rng)
        response = connection.call_raw(bodies[query["id"]])
        records.append(
            {"client": client, "t_start": now - window_start, "latency_ms": response.latency_ms, "class": query["class"], "status": status(response)}
        )
    if cpu_start is None:
        return records, 0.0
    return records, (time.process_time() - cpu_start[0]) / (time.perf_counter() - cpu_start[1])


def parse_mix(text: str) -> dict[str, float]:
    """Returns the `heavy` mix, or the class weights of a JSON file."""
    if text == "heavy":
        return dict(HEAVY)
    with open(text) as file:
        weights = json.load(file)
    if not isinstance(weights, dict) or not all(isinstance(w, (int, float)) and w > 0 for w in weights.values()):
        raise ValueError(f"{text}: expected a JSON object of positive class weights")
    return {str(cls): float(w) for cls, w in weights.items()}


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("load", help="concurrent clients on a weighted mix for a fixed time (G8)")
    meta.add_node_args(parser)
    meta.add_result_args(parser)
    parser.add_argument("--queries", required=True, help="a query file from sample (g7.jsonl)")
    parser.add_argument("--mix", type=parse_mix, default="heavy", help="heavy, or a JSON file of class weights")
    parser.add_argument("--clients", default="1,8,32,64", help="client counts, run one after another")
    parser.add_argument("--warmup", type=float, default=20, help="seconds before the window, not measured")
    parser.add_argument("--duration", type=float, default=120, help="seconds measured per client count")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=rpc.DEFAULT_TIMEOUT_S, help="per-request timeout in seconds")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    header, queries = sample.read_queries(args.queries)
    if header.get("kind") == "oneshot":
        raise SystemExit("oneshot.jsonl never goes into a load mix")
    counts = [int(n) for n in args.clients.split(",")]
    mix = Mix(queries, args.mix)
    if not mix.classes:
        raise SystemExit(f"{args.queries} has no query of the mix's classes at most {MAX_WIDTH} blocks wide")
    if args.mode == "index":
        run.check_window(run.query_ranges([q for qs in mix.queries.values() for q in qs]), prom.scrape(args.metrics))
    if mix.missing:
        print(f"load: no query for {', '.join(mix.missing)}; the other weights are used as given")
    info = meta.collect(
        "load", args.mode, "warm", args.unit,
        kind=header.get("kind"), queries=meta.file_info(args.queries), mix=args.mix, clients=counts,
        warmup_s=args.warmup, duration_s=args.duration, seed=args.seed, timeout_s=args.timeout,
    )
    directory = meta.run_dir(args.out, info, "load")
    meta.write(directory, info)
    with open(os.path.join(directory, "load.jsonl"), "w") as out:
        for clients in counts:
            start_at = time.time() + START_DELAY_S + START_DELAY_PER_CLIENT_S * clients
            jobs = [
                (n, args.rpc, args.timeout, queries, args.mix, f"{args.seed}-{clients}-{n}", start_at, args.warmup, args.duration)
                for n in range(clients)
            ]
            with multiprocessing.Pool(clients) as pool:
                results = pool.starmap(client_loop, jobs)
            records = sorted((r for rs, _ in results for r in rs), key=lambda r: r["t_start"])
            for record in records:
                out.write(json.dumps({"clients": clients, **record}) + "\n")
            summary = summarize(records, clients, args.duration, args.warmup, [cpu for _, cpu in results], mix.missing)
            out.write(json.dumps(summary) + "\n")
            out.flush()
            flag = " CLIENT CPU FLAG: the client may be the bottleneck" if summary["client_cpu_flag"] else ""
            print(f"{clients} clients: {summary['qps']:.2f} QPS, p99 {summary['latency_ms']['all']['p99']} ms, {summary['statuses']}{flag}")
    info["ended"] = meta.now()
    meta.write(directory, info)
    print(directory)
    return 0
