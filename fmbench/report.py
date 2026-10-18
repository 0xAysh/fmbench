"""`report`: the G1, G4, G7 and G8 tables, the sweep, one-shot and build tables, the chart CSVs
and the gate verdicts of one commit's results.

Verdicts are `pass`, `fail`, `equal` (a ratio is within the run-to-run spread of its limit, D4)
and `not measured` (missing data, an error on either path, or different results).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass, field
from typing import Any

from fmbench import equal, load, meta, prom, rpc, run, sample, stats

PASS, FAIL, EQUAL, NOT_MEASURED = "pass", "fail", "equal", "not measured"
# The verdict of a set of cells is the worst one.
SEVERITY = (FAIL, NOT_MEASURED, EQUAL, PASS)
# The verdict of a G4 row that the others are measured against.
REFERENCE = "reference"
DEFAULT_GATES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gates.json")
# The counters the tables show per mode, as mean per request over both paths.
WORK = ("headers_read", "receipt_sets_read", "matching_blocks", "logs")
# The G4 rows: the `watch` phase and mode of each, and the phase and mode of its reference. A load
# is compared with the same load with the index off, not with an idle node.
G4_PHASES: tuple[tuple[str, str, tuple[str, str] | None], ...] = (
    ("off", "bloom", None),
    ("catchup", "index", ("off", "bloom")),
    ("load", "bloom", None),
    ("load", "index", ("load", "bloom")),
)

Cells = dict[tuple[str, sample.Width], "Cell"]


@dataclass
class Cell:
    """The requests of one class × width in one run."""

    latencies: list[float] = field(default_factory=list)
    errors: int = 0
    requests: int = 0
    # The digest of each query's first repetition.
    digests: dict[str, Any] = field(default_factory=dict)
    counters: dict[str, dict[str, float]] = field(default_factory=dict)

    def p(self, q: float) -> float | None:
        """Returns the `q`-quantile of the answered requests' latencies, or `None` for none."""
        return stats.percentile(self.latencies, q)

    def work(self, name: str) -> float | None:
        """Returns the mean per request of `reth_rpc_eth_filter_<name>_total` over both paths."""
        paths = self.counters.get(f"{prom.RPC_FILTER_PREFIX}{name}_total")
        return sum(paths.values()) / self.requests if paths and self.requests else None


def cells(lines: list[dict[str, Any]]) -> Cells:
    """Groups `seq.jsonl` lines by class × width."""
    out: Cells = {}
    for line in sorted(lines, key=lambda line: line["rep"]):
        cell = out.setdefault(sample.query_cell(line["query_id"]), Cell())
        cell.requests += 1
        if line["error"] is None:
            cell.latencies.append(line["latency_ms"])
        else:
            cell.errors += 1
        cell.digests.setdefault(line["query_id"], line["digest"])
        run.add_counters(cell.counters, line["counters"])
    return out


def spreads(a: Cells, b: Cells, floor: float) -> dict[tuple[str, sample.Width], tuple[float, float]]:
    """Returns the spread of p50 and of p90 per cell from two runs of the same query file, mode and
    cache state: the relative difference (over the smaller value), at least `floor`. A cell with an
    error in either run keeps the floor."""
    out = {}
    for key in a.keys() & b.keys():
        x, y = a[key], b[key]
        if x.errors or y.errors:
            continue
        s50, s90 = _relative(x.p(0.5), y.p(0.5)), _relative(x.p(0.9), y.p(0.9))
        if s50 is not None and s90 is not None:
            out[key] = (max(floor, s50), max(floor, s90))
    return out


def _relative(x: float | None, y: float | None) -> float | None:
    """Returns |x − y| over the smaller of the two, or `None` without two positive values."""
    if x is None or y is None or min(x, y) <= 0:
        return None
    return abs(x - y) / min(x, y)


def spread_pair(runs: list[dict[str, Any]], mode: str, cache: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Returns the latest g7 run of `mode` and `cache` that the latest spread run repeats (the
    same query file, query count and repetitions) and that spread run, or `None`."""
    repeat = pick(runs, "run", mode, cache, spread=True, kind="g7")
    if repeat is None:
        return None
    base = pick(runs, "run", mode, cache, kind="g7", **_same_queries(repeat))
    return (base, repeat) if base else None


def g7_runs(runs: list[dict[str, Any]], cache: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Returns the latest index g7 run of `cache` and the latest bloom and today g7 runs of the
    same query file, query count and repetitions, each `None` when missing (all three without an
    index run). A newer smoke run with `--limit` is never compared with the full matrix."""
    index = pick(runs, "run", "index", cache, kind="g7")
    if index is None:
        return None, None, None
    same = _same_queries(index)
    return index, pick(runs, "run", "bloom", cache, kind="g7", **same), pick(runs, "run", "today", cache, kind="g7", **same)


def _same_queries(found: dict[str, Any]) -> dict[str, Any]:
    """Returns the meta fields that a run compared with `found` must match: the query file, the
    query count and the repetitions."""
    return {key: found["meta"][key] for key in ("queries", "queries_used", "reps")}


def fell_back(cls: str, counters: dict[str, dict[str, float]]) -> bool:
    """Whether the index did not serve all of an index-mode request of class `cls`: a lookup
    failed (`fallbacks`), or the header bloom served a window, for part of the range (the index
    tail moved under the query) or all of it (a wiped store has no indexed range). The wildcard
    (`sample.WILDCARD`) is never looked up, so all its windows are bloom windows by design."""
    bloom_windows = counters.get(prom.RPC_FILTER_PREFIX + "windows_total", {}).get("bloom", 0)
    fallbacks = sum(counters.get(prom.RPC_FILTER_PREFIX + "fallbacks_total", {}).values())
    return fallbacks > 0 or (cls != sample.WILDCARD and bloom_windows > 0)


def d2_check(index_ms: float | None, index_error: Any, bloom_p50_ms: float | None) -> str:
    """The D2 condition: the index path's whole-window Q1 request costs no more than the bloom
    path's p50 of the same filter at 100,000 blocks. A timeout on the index path fails it. An
    index request that fell back to the header bloom (`fell_back`) comes in as `index_ms=None`,
    so it is not measured."""
    if index_error == rpc.TIMEOUT:
        return FAIL
    if index_error is not None or index_ms is None or bloom_p50_ms is None:
        return NOT_MEASURED
    return PASS if index_ms <= bloom_p50_ms else FAIL


def _outcome(ratio: float, spread: float, limit: float, higher_is_better: bool = False) -> str:
    """Judges one index/bloom ratio against its limit: `equal` from `limit / (1 + spread)` to
    `limit × (1 + spread)`, where the noise cannot tell them apart, else `pass` on the better side
    and `fail` on the worse one. The spread is the larger of two runs over the smaller, less 1, so
    it scales a ratio both ways. A worse ratio never gets a better verdict."""
    low, high = limit / (1 + spread), limit * (1 + spread)
    if low <= ratio <= high:
        return EQUAL
    return PASS if (ratio > high if higher_is_better else ratio < low) else FAIL


def _combine(outcomes: list[str]) -> str:
    """Returns the verdict of a cell from its ratios: `fail` if one fails, `pass` if all pass,
    else `equal`."""
    if FAIL in outcomes:
        return FAIL
    return PASS if all(outcome == PASS for outcome in outcomes) else EQUAL


def g8_verdict(index: dict[str, Any] | None, bloom: dict[str, Any] | None, gates: dict[str, Any]) -> str:
    """G8 at one client count: index QPS at least the bloom QPS and index p99 at most the bloom
    p99, each ratio judged on its own with the spread floor around its limit. A flagged client,
    any non-`ok` request, a missing run, no bloom answer or no latency is `not measured`."""
    if index is None or bloom is None:
        return NOT_MEASURED
    if any(s["client_cpu_flag"] or set(s["statuses"]) - {"ok"} for s in (index, bloom)):
        return NOT_MEASURED
    qps = _ratio(index["qps"], bloom["qps"])
    p99 = _ratio(index["latency_ms"]["all"]["p99"], bloom["latency_ms"]["all"]["p99"])
    if qps is None or p99 is None:
        return NOT_MEASURED
    rule, floor = gates["g8"], gates["spread_floor"]
    return _combine([_outcome(qps, floor, rule["qps_ratio_min"], higher_is_better=True), _outcome(p99, floor, rule["p99_ratio_max"])])


def g4_verdict(reference_ms: list[float], measured_ms: list[float], gates: dict[str, Any]) -> tuple[float | None, float | None, str]:
    """G4: block-time p50 and p99 at most 1.05 × the reference, or within the spread, whichever
    is larger. Returns both ratios and the verdict; no blocks on either side is `not measured`."""
    r50 = _ratio(stats.percentile(measured_ms, 0.5), stats.percentile(reference_ms, 0.5))
    r99 = _ratio(stats.percentile(measured_ms, 0.99), stats.percentile(reference_ms, 0.99))
    if r50 is None or r99 is None:
        return None, None, NOT_MEASURED
    rule, floor = gates["g4"], gates["spread_floor"]
    ok = r50 <= max(rule["p50_ratio_max"], 1 + floor) and r99 <= max(rule["p99_ratio_max"], 1 + floor)
    return r50, r99, PASS if ok else FAIL


def g4_rows(block_ms: dict[tuple[str, str], list[float]], gates: dict[str, Any]) -> list[list[Any]]:
    """Returns one G4 row per `G4_PHASES` entry from the block times (ms) of each `watch` phase
    and mode: phase, mode, blocks, p50, p99, both ratios to its reference and the verdict."""
    rows = []
    for phase, mode, reference in G4_PHASES:
        values = block_ms.get((phase, mode), [])
        r50, r99, verdict = g4_verdict(block_ms.get(reference, []), values, gates) if reference else (None, None, REFERENCE)
        rows.append([phase, mode, len(values), _ms(stats.percentile(values, 0.5)), _ms(stats.percentile(values, 0.99)), _num(r50, 3), _num(r99, 3), verdict])
    return rows


def class_type(cls: str, gates: dict[str, Any]) -> str | None:
    """Returns `selective`, `dense` or `equal`: the G7 rule of a class."""
    return next((kind for kind, rule in gates["g7"].items() if cls in rule["classes"]), None)


def g7_verdict(kind: str, width: sample.Width, r50: float, r90: float, s50: float, s90: float, gates: dict[str, Any]) -> str:
    """Applies the G7 rule of a class type to the index/bloom ratios of p50 and p90, given the
    cell's spread of each. Each ratio is judged on its own: within its spread of its limit it is
    `equal`, never a fail; outside, it passes or fails against the limit. The cell fails if a
    ratio fails, passes if all pass, and is `equal` otherwise. For an equal class (the wildcard,
    which both paths serve from the header bloom) both ratios within the spread of 1 is the pass
    condition. Dense classes check p50 only."""
    rule = gates["g7"][kind]
    if kind == "equal":
        return PASS if _outcome(r50, s50, 1.0) == EQUAL and _outcome(r90, s90, 1.0) == EQUAL else FAIL
    if kind == "dense":
        return _outcome(r50, s50, rule["p50_ratio_max"])
    p50_max = rule["p50_ratio_max"]
    if width == sample.WINDOW or (isinstance(width, int) and width >= rule["wide_width_min"]):
        p50_max = min(p50_max, rule["wide_p50_ratio_max"])
    return _combine([_outcome(r50, s50, p50_max), _outcome(r90, s90, rule["p90_ratio_max"])])


def g7_rows(
    index: Cells,
    bloom: Cells,
    spreads: dict[tuple[str, sample.Width], tuple[float, float]],
    gates: dict[str, Any],
    today: Cells | None = None,
) -> list[dict[str, Any]]:
    """Returns one G7 row per class × width of either run, with its verdict."""
    floor = gates["spread_floor"]
    rows = []
    for key in sorted(index.keys() | bloom.keys(), key=_cell_order):
        cls, width = key
        a, b = index.get(key), bloom.get(key)
        s50, s90 = spreads.get(key, (floor, floor))
        row: dict[str, Any] = {"class": cls, "width": width, "spread_p50": s50, "spread_p90": s90, "note": ""}
        for mode, cell in (("index", a), ("bloom", b), ("today", (today or {}).get(key))):
            row[f"{mode}_p50"] = cell.p(0.5) if cell else None
            row[f"{mode}_p90"] = cell.p(0.9) if cell else None
            for name in WORK:
                row[f"{mode}_{name}"] = cell.work(name) if cell else None
        kind = class_type(cls, gates)
        row["p50_ratio"] = row["p90_ratio"] = None
        if a is None or b is None or kind is None:
            row["verdict"], row["note"] = NOT_MEASURED, "missing on a path" if kind else "no G7 rule"
        elif a.errors or b.errors:
            row["verdict"], row["note"] = NOT_MEASURED, f"errors: index {a.errors}, bloom {b.errors}"
        elif fell_back(cls, a.counters):
            row["verdict"], row["note"] = NOT_MEASURED, "the index path fell back to the header bloom"
        elif a.digests != b.digests:
            row["verdict"], row["note"] = NOT_MEASURED, "results differ between the paths (G1)"
        else:
            row["p50_ratio"] = _ratio(a.p(0.5), b.p(0.5))
            row["p90_ratio"] = _ratio(a.p(0.9), b.p(0.9))
            if row["p50_ratio"] is None or row["p90_ratio"] is None:
                row["verdict"], row["note"] = NOT_MEASURED, "no latency"
            else:
                row["verdict"] = g7_verdict(kind, width, row["p50_ratio"], row["p90_ratio"], s50, s90, gates)
        rows.append(row)
    return rows


def missing_classes(rows: list[dict[str, Any]], gates: dict[str, Any]) -> list[str]:
    """Returns the classes with a G7 rule but no row: not sampled, or above the result cap at
    every width."""
    present = {row["class"] for row in rows}
    return sorted(cls for rule in gates["g7"].values() for cls in rule["classes"] if cls not in present)


def sweep_mean(summaries: list[dict[str, Any]]) -> dict[str, float] | None:
    """Returns the number of completed runs of one sweep (`completed`) and their mean total time,
    blocks/s and requests/s, or `None` with fewer than two."""
    completed = [s for s in summaries if s["completed"]]
    if len(completed) < 2:
        return None
    means = {key: sum(s[key] for s in completed) / len(completed) for key in ("total_ms", "blocks_per_s", "requests_per_s")}
    return {"completed": len(completed), **means}


def _cell_order(key: tuple[str, sample.Width]) -> tuple[str, int]:
    cls, width = key
    return cls, width if isinstance(width, int) else 1 << 62


def worst(verdicts: list[str]) -> str:
    """Returns the gate verdict of a set of cells: any fail fails, then any `not measured`, then
    any `equal`. No cell at all is `not measured`."""
    return min(verdicts, key=SEVERITY.index) if verdicts else NOT_MEASURED


# Everything below reads the result directories and writes the report.


def load_runs(root: str) -> list[dict[str, Any]]:
    """Returns every run directory under `root` (one commit) with its `meta.json`."""
    runs = []
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name, "meta.json")
        if os.path.isfile(path):
            with open(path) as file:
                runs.append({"dir": os.path.dirname(path), "meta": json.load(file)})
    return runs


def pick(runs: list[dict[str, Any]], command: str, mode: str, cache: str = "warm", spread: bool = False, **match: Any) -> dict[str, Any] | None:
    """Returns the latest finished run of `command` in `mode` and `cache` whose meta has `match`."""
    found = [
        r
        for r in runs
        if r["meta"]["command"] == command
        and r["meta"]["mode"] == mode
        and r["meta"]["cache"] == cache
        and r["meta"]["ended"] is not None
        and bool(r["meta"].get("spread")) == spread
        and all(r["meta"].get(k) == v for k, v in match.items())
    ]
    return max(found, key=lambda r: r["meta"]["started"]) if found else None


def read_csv(path: str) -> list[dict[str, str]]:
    """Reads a CSV file written by `watch` into one dict per row."""
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def seq_cells(found: dict[str, Any] | None) -> Cells | None:
    """Returns the cells of a run's `seq.jsonl`, or `None` for no run."""
    return cells(meta.read_jsonl(os.path.join(found["dir"], "seq.jsonl"))) if found else None


def build(root: str, gates: dict[str, Any], today_root: str | None = None) -> tuple[str, dict[str, list[list[Any]]]]:
    """Returns `report.md` and the chart CSVs (name → rows, header first) of one commit.
    `today_root` is the results directory of the Reth 2.7.0 commit (run C), whose `today` runs
    fill the today columns."""
    runs = load_runs(root)
    if today_root is not None:
        runs += [r for r in load_runs(today_root) if r["meta"]["mode"] == "today"]
    report = _Report(runs, gates, os.path.basename(os.path.normpath(root)), today_root)
    for section in (report.g7, report.g8, report.g1, report.g4, report.sweeps, report.oneshot, report.index_build, report.summary):
        section()
    return "\n".join(report.md) + "\n", report.charts


class _Report:
    """The sections of one commit's report, written in order into `md` and `charts`."""

    def __init__(self, runs: list[dict[str, Any]], gates: dict[str, Any], commit: str, today_root: str | None) -> None:
        self.runs, self.gates, self.commit = runs, gates, commit
        self.floor = gates["spread_floor"]
        today_source = f"the `today` runs in `{today_root}`" if today_root else "none (pass `--today results/<2.7.0 commit>/`)"
        self.md = [f"# FilterMaps bench report: commit {commit}", "", f"Gates: `gates.json`, spread floor {self.floor:.0%}. Reth 2.7.0 (today): {today_source}.", ""]
        self.charts: dict[str, list[list[Any]]] = {
            "latency_vs_width.csv": [["class", "width", "mode", "cache", "p50_ms", "p90_ms"]],
            "work_vs_width.csv": [["class", "width", "mode", "headers_read", "receipt_sets_read"]],
            "throughput.csv": [["clients", "mode", "qps", "p50_ms", "p95_ms", "p99_ms"]],
            "sweep.csv": [["filter_id", "chunk", "mode", "run", "requests", "total_s", "blocks_per_s", "requests_per_s", "logs", "headers_read", "receipt_sets_read", "completed"]],
            "build.csv": [["elapsed_s", "blocks_per_s", "maps_per_s", "store_bytes", "bytes_per_block", "lag_blocks", "lag_maps"]],
        }
        self.verdicts: dict[str, str] = {}

    def heading(self, title: str) -> None:
        self.md += [f"## {title} (commit {self.commit})", ""]

    def spread(self, cache: str) -> dict[tuple[str, sample.Width], tuple[float, float]]:
        """The per-cell spread of one cache state (D4): each spread run against the run it
        repeats, the larger of the two modes."""
        merged: dict[tuple[str, sample.Width], tuple[float, float]] = {}
        sources = []
        for mode in ("index", "bloom"):
            pair = spread_pair(self.runs, mode, cache)
            if pair is None:
                continue
            sources.append(mode)
            base, repeat = pair
            for key, (s50, s90) in spreads(seq_cells(base) or {}, seq_cells(repeat) or {}, self.floor).items():
                old = merged.get(key, (0.0, 0.0))
                merged[key] = (max(old[0], s50), max(old[1], s90))
        source = f"measured from the {' and '.join(sources)} spread runs" if sources else f"no spread run, so the floor ({self.floor:.0%}) applies to every cell"
        self.md += [f"Spread: {source}.", ""]
        return merged

    def g7(self) -> None:
        verdicts = []
        all_rows = []
        for cache in ("warm", "cold"):
            index, bloom, today = (seq_cells(found) for found in g7_runs(self.runs, cache))
            if index is None:
                continue
            self.heading(f"G7 single-query latency, {cache}")
            rows = g7_rows(index, bloom or {}, self.spread(cache), self.gates, today)
            verdicts += [row["verdict"] for row in rows]
            all_rows += rows
            self.md += _table(
                ["class", "width", "index p50", "index p90", "bloom p50", "bloom p90", "today p50", "p50 ratio", "p90 ratio", "spread p50/p90",
                 "index headers", "index receipts", "bloom headers", "bloom receipts", "matching blocks", "logs", "verdict", "note"],
                [[r["class"], r["width"], _ms(r["index_p50"]), _ms(r["index_p90"]), _ms(r["bloom_p50"]), _ms(r["bloom_p90"]), _ms(r["today_p50"]),
                  _num(r["p50_ratio"], 2), _num(r["p90_ratio"], 2), f"{r['spread_p50']:.0%}/{r['spread_p90']:.0%}",
                  _num(r["index_headers_read"], 1), _num(r["index_receipt_sets_read"], 1), _num(r["bloom_headers_read"], 1), _num(r["bloom_receipt_sets_read"], 1),
                  _num(r["index_matching_blocks"], 1), _num(r["index_logs"], 1), r["verdict"], r["note"]] for r in rows],
            )
            self.md += ["Latency in ms. Work is the mean per request over both paths; on the index path, headers read are the candidate blocks.", ""]
            for r in rows:
                for mode in ("index", "bloom", "today"):
                    if r[f"{mode}_p50"] is not None:
                        self.charts["latency_vs_width.csv"].append([r["class"], r["width"], mode, cache, r[f"{mode}_p50"], r[f"{mode}_p90"]])
                        if cache == "warm":
                            self.charts["work_vs_width.csv"].append([r["class"], r["width"], mode, r[f"{mode}_headers_read"], r[f"{mode}_receipt_sets_read"]])
        missing = missing_classes(all_rows, self.gates)
        if all_rows and missing:
            self.md += [f"Classes without a G7 cell: {', '.join(missing)} (not sampled, or above the result cap at every width: `dropped_by_cap` in the query file).", ""]
        self.verdicts["G7"] = worst(verdicts)

    def g8(self) -> None:
        summaries: dict[str, dict[int, dict[str, Any]]] = {}
        for mode in ("index", "bloom", "today"):
            found = pick(self.runs, "load", mode)
            lines = meta.read_jsonl(os.path.join(found["dir"], "load.jsonl")) if found else []
            summaries[mode] = {line["clients"]: line for line in lines if line.get("summary")}
            for clients, line in sorted(summaries[mode].items()):
                latency = line["latency_ms"]["all"]
                self.charts["throughput.csv"].append([clients, mode, line["qps"], latency["p50"], latency["p95"], latency["p99"]])
        rows: list[list[Any]] = []
        for clients in self.gates["g8"]["clients"]:
            pair = (summaries["index"].get(clients), summaries["bloom"].get(clients))
            qps = [s["qps"] if s else None for s in pair]
            p99 = [s["latency_ms"]["all"]["p99"] if s else None for s in pair]
            rows.append([clients, _num(qps[0], 2), _num(qps[1], 2), _ms(p99[0]), _ms(p99[1]), _num(_ratio(*qps), 2), _num(_ratio(*p99), 2), g8_verdict(*pair, self.gates)])
        self.verdicts["G8"] = worst([row[-1] for row in rows])
        self.heading("G8 throughput, heavy mix")
        self.md += _table(["clients", "index QPS", "bloom QPS", "index p99 ms", "bloom p99 ms", "QPS ratio", "p99 ratio", "verdict"], rows)
        self.md += [f"A flagged client (above {load.CPU_FLAG:.0%} of a core) or any request that was not `ok` makes a row `not measured`. G8 also needs G4 under load.", ""]

    def g1(self) -> None:
        self.heading("G1 correctness")
        a, b = (pick(self.runs, "run", mode, kind="g1") for mode in ("index", "bloom"))
        if a is None or b is None:
            self.verdicts["G1"] = NOT_MEASURED
            self.md += ["No g1 run in both index and bloom mode: **not measured**.", ""]
            return
        compared, differences = equal.compare(os.path.join(a["dir"], "seq.jsonl"), os.path.join(b["dir"], "seq.jsonl"))
        minimum = self.gates["g1"]["queries_min"]
        if len(differences) > self.gates["g1"]["differences_max"]:
            self.verdicts["G1"] = FAIL
        else:
            self.verdicts["G1"] = PASS if compared >= minimum else NOT_MEASURED
        reached = "reached" if compared >= minimum else "not reached"
        self.md += [f"{compared} queries compared, {len(differences)} differences; queries_min {minimum} {reached}: **{self.verdicts['G1']}**."]
        self.md += [f"- {d}" for d in differences] + [""]

    def g4(self) -> None:
        block_ms = {}
        for phase, mode, _ in G4_PHASES:
            found = pick(self.runs, "watch", mode, phase=phase)
            block_ms[(phase, mode)] = [float(row["elapsed_ms"]) for row in read_csv(os.path.join(found["dir"], "blocks.csv"))] if found else []
        rows = g4_rows(block_ms, self.gates)
        self.verdicts["G4"] = worst([row[-1] for row in rows if row[-1] != REFERENCE])
        self.heading("G4 node cost: block execution time")
        self.md += _table(["phase", "mode", "blocks", "p50 ms", "p99 ms", "p50 ratio", "p99 ratio", "verdict"], rows)
        limit = self.gates["g4"]["p50_ratio_max"]
        self.md += [f"The catch-up is measured against the index-off phase, the index-mode load against the same load in bloom mode. "
                    f"The limit is {limit} × or 1 + the spread floor, whichever is larger.", ""]

    def sweeps(self) -> None:
        rows: list[list[Any]] = []
        found_runs = [r for r in self.runs if r["meta"]["command"] == "sweep" and r["meta"]["ended"]]
        for found in sorted(found_runs, key=lambda r: (r["meta"]["filter_id"], r["meta"]["chunk"], r["meta"]["mode"])):
            mode = found["meta"]["mode"]
            lines = [line for line in meta.read_jsonl(os.path.join(found["dir"], "sweep.jsonl")) if line.get("summary")]
            for line in lines:
                work = [sum(line["counters"].get(f"{prom.RPC_FILTER_PREFIX}{name}_total", {}).values()) for name in ("headers_read", "receipt_sets_read")]
                values: list[Any] = [line["filter_id"], line["chunk"], mode, line["run"], line["requests"], line["total_ms"] / 1000,
                                     line["blocks_per_s"], line["requests_per_s"], line["logs"], *work, line["completed"]]
                self.charts["sweep.csv"].append(values)
                rows.append(values)
            means = sweep_mean(lines)
            if means:
                rows.append([lines[0]["filter_id"], lines[0]["chunk"], mode, f"mean of {means['completed']} completed", lines[0]["planned_requests"],
                             means["total_ms"] / 1000, means["blocks_per_s"], means["requests_per_s"], None, None, None, None])
        self.heading("Sweeps")
        self.md += _table(["filter", "chunk", "mode", "run", "requests", "total s", "blocks/s", "requests/s", "logs", "headers read", "receipt sets read", "completed"],
                          [[_num(v, 1) if isinstance(v, float) else v for v in row] for row in rows])
        self.md += ["The kit runs on the node's machine: a request has no network delay. A remote indexer adds one round trip per request to both paths.", ""]

    def oneshot(self) -> None:
        self.heading("One-shot over the whole window (D2)")
        shots: dict[str, dict[str, tuple[dict[str, Any], float]]] = {}
        window = None
        for mode in ("index", "bloom"):
            found = pick(self.runs, "run", mode, kind="oneshot")
            if found is None:
                continue
            window = found["meta"].get("window") or window
            for line in meta.read_jsonl(os.path.join(found["dir"], "seq.jsonl")):
                shots.setdefault(line["query_id"], {})[mode] = (line, found["meta"]["timeout_s"])
        if window:
            self.md.append(f"Over the query file's window, {window[1] - window[0] + 1} blocks ({window[0]}..{window[1]}): "
                           "the indexed window less the sample's tail margin.")
        for query_id, by_mode in sorted(shots.items()):
            parts = []
            for mode, (line, timeout_s) in sorted(by_mode.items()):
                if line["error"] == rpc.TIMEOUT:
                    parts.append(f"{mode} > {timeout_s / 60:.0f} min")
                else:
                    parts.append(f"{mode} " + (f"error {line['error']}" if line["error"] else f"{line['latency_ms']:.1f} ms"))
                if mode == "index" and fell_back(sample.query_cell(query_id)[0], line["counters"]):
                    parts[-1] += " (fell back to the header bloom)"
            self.md.append(f"- {query_id}: {', '.join(parts)}")
        self.md.append("")
        q1_id = next((query_id for query_id in sorted(shots) if query_id.startswith("Q1-")), None)
        bloom_width = self.gates["d2"]["bloom_width"]
        bloom_p50 = None
        # The bloom run of the warm G7 table.
        found = g7_runs(self.runs, "warm")[1]
        if found and q1_id:
            # The one-shot holds the first Q1 filter of g7.jsonl: the same filter index.
            reference = f"Q1-w{bloom_width}-{q1_id.split('-')[2]}"
            lines = meta.read_jsonl(os.path.join(found["dir"], "seq.jsonl"))
            bloom_p50 = stats.percentile([line["latency_ms"] for line in lines if line["query_id"] == reference and line["error"] is None], 0.5)
        index_line = shots.get(q1_id or "", {}).get("index", (None, None))[0]
        index_ms = index_line["latency_ms"] if index_line and not fell_back("Q1", index_line["counters"]) else None
        self.verdicts["D2"] = d2_check(index_ms, index_line["error"] if index_line else None, bloom_p50)
        self.md += [f"D2: the index whole-window Q1 against the bloom p50 of the same filter at {bloom_width} blocks ({_ms(bloom_p50)} ms): "
                    f"**{self.verdicts['D2']}**. PR #11 T11 lands only on a pass.", ""]

    def index_build(self) -> None:
        self.heading("Index build")
        found = pick(self.runs, "watch", "index", phase="catchup")
        rows = read_csv(os.path.join(found["dir"], "watch.csv")) if found else []
        for row in rows:
            self.charts["build.csv"].append([row[k] for k in ("elapsed_s", "blocks_per_s", "maps_per_s", "store_bytes", "bytes_per_block", "lag_blocks", "lag_maps")])
        fm = prom.FILTER_MAPS_PREFIX
        rendered = [(float(r["elapsed_s"]), float(r[fm + "blocks_rendered"]), float(r[fm + "maps_written"])) for r in rows if r.get(fm + "blocks_rendered")]
        if len(rendered) < 2:
            self.md += ["No catch-up watch with two samples of the index series.", ""]
            return
        seconds = rendered[-1][0] - rendered[0][0]
        self.md += _table(
            ["blocks/s", "maps/s", "store bytes", "bytes per indexed block"],
            [[_num((rendered[-1][1] - rendered[0][1]) / seconds, 1), _num((rendered[-1][2] - rendered[0][2]) / seconds, 2), rows[-1]["store_bytes"], _num(_float(rows[-1]["bytes_per_block"]), 1)]],
        )

    def summary(self) -> None:
        self.heading("Gate verdicts")
        self.md += _table(["gate", "verdict"], [[gate, verdict] for gate, verdict in self.verdicts.items()])


def _table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join("—" if v is None else str(v) for v in row) + " |" for row in rows]
    return lines + [""]


def _ms(value: Any) -> str | None:
    return None if value is None else f"{value:.1f}"


def _num(value: Any, digits: int) -> Any:
    return value if value is None or isinstance(value, (str, bool)) else f"{value:.{digits}f}"


def _ratio(a: float | None, b: float | None) -> float | None:
    return a / b if a is not None and b else None


def _float(text: str) -> float | None:
    return float(text) if text else None


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("report", help="tables, chart CSVs and gate verdicts of one commit's results")
    parser.add_argument("root", help="results/<commit>/")
    parser.add_argument("--today", help="results/<commit>/ of the Reth 2.7.0 binary, whose today-mode runs fill the today columns")
    parser.add_argument("--gates", default=DEFAULT_GATES, help="the gate thresholds (default: the kit's gates.json)")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    with open(args.gates) as file:
        gates = json.load(file)
    text, charts = build(args.root, gates, args.today)
    with open(os.path.join(args.root, "report.md"), "w") as file:
        file.write(text)
    for name, rows in charts.items():
        with open(os.path.join(args.root, name), "w", newline="") as file:
            csv.writer(file).writerows(rows)
    print(os.path.join(args.root, "report.md"))
    return 0
