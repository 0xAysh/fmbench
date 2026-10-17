"""`report`: the G1, G4, G7 and G8 tables, the sweep, one-shot and build tables, the chart CSVs
and the gate verdicts of one commit's results.

Verdicts are `pass`, `fail`, `equal` (the difference is inside the run-to-run spread, D4) and
`not measured` (missing data, an error on either path, or different results).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass, field
from typing import Any

from fmbench import equal, prom, rpc, run, sample, stats

PASS, FAIL, EQUAL, NOT_MEASURED = "pass", "fail", "equal", "not measured"
# The verdict of a set of cells is the worst one.
SEVERITY = (FAIL, NOT_MEASURED, EQUAL, PASS)
DEFAULT_GATES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gates.json")
# The counters the tables show per mode, as mean per request over both paths.
WORK = ("headers_read", "receipt_sets_read", "matching_blocks", "logs")

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
        return stats.percentile(self.latencies, q)

    def work(self, name: str) -> float | None:
        """Returns the mean per request of `reth_rpc_eth_filter_<name>_total` over both paths."""
        paths = self.counters.get(f"reth_rpc_eth_filter_{name}_total")
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
        if x.errors or y.errors or not x.latencies or not y.latencies:
            continue
        out[key] = (max(floor, _relative(x.p(0.5), y.p(0.5))), max(floor, _relative(x.p(0.9), y.p(0.9))))
    return out


def _relative(x: float | None, y: float | None) -> float:
    assert x is not None and y is not None
    return abs(x - y) / min(x, y)


def d2_check(index_ms: float | None, index_error: Any, bloom_p50_ms: float | None) -> str:
    """The D2 condition: the index path's whole-window Q1 request costs no more than the bloom
    path's p50 of the same filter at 100,000 blocks. A timeout on the index path fails it."""
    if index_error == rpc.TIMEOUT:
        return FAIL
    if index_error is not None or index_ms is None or bloom_p50_ms is None:
        return NOT_MEASURED
    return PASS if index_ms <= bloom_p50_ms else FAIL


def g8_verdict(index: dict[str, Any] | None, bloom: dict[str, Any] | None, gates: dict[str, Any]) -> str:
    """G8 at one client count: index QPS at least the bloom QPS and index p99 at most the bloom
    p99. A flagged client, any non-`ok` request or a missing run is `not measured`."""
    if index is None or bloom is None:
        return NOT_MEASURED
    if any(s["client_cpu_flag"] or set(s["statuses"]) - {"ok"} for s in (index, bloom)):
        return NOT_MEASURED
    rule, floor = gates["g8"], gates["spread_floor"]
    qps = index["qps"] / bloom["qps"]
    p99 = index["latency_ms"]["all"]["p99"] / bloom["latency_ms"]["all"]["p99"]
    if abs(qps - 1) <= floor and abs(p99 - 1) <= floor:
        return EQUAL
    return PASS if qps >= rule["qps_ratio_min"] and p99 <= rule["p99_ratio_max"] else FAIL


def g4_verdict(reference_ms: list[float], measured_ms: list[float], gates: dict[str, Any]) -> tuple[float | None, float | None, str]:
    """G4: block-time p50 and p99 at most 1.05 × the index-off reference, or within the spread,
    whichever is larger. Returns both ratios and the verdict."""
    if not reference_ms or not measured_ms:
        return None, None, NOT_MEASURED
    r50 = _relative_to(measured_ms, reference_ms, 0.5)
    r99 = _relative_to(measured_ms, reference_ms, 0.99)
    rule, floor = gates["g4"], gates["spread_floor"]
    ok = r50 <= max(rule["p50_ratio_max"], 1 + floor) and r99 <= max(rule["p99_ratio_max"], 1 + floor)
    return r50, r99, PASS if ok else FAIL


def _relative_to(measured: list[float], reference: list[float], q: float) -> float:
    value, base = stats.percentile(measured, q), stats.percentile(reference, q)
    assert value is not None and base is not None
    return value / base


def class_type(cls: str, gates: dict[str, Any]) -> str | None:
    """Returns `selective`, `dense` or `equal`: the G7 rule of a class."""
    return next((kind for kind, rule in gates["g7"].items() if cls in rule["classes"]), None)


def g7_verdict(kind: str, width: sample.Width, r50: float, r90: float, s50: float, s90: float, gates: dict[str, Any]) -> str:
    """Applies the G7 rule of a class type to the index/bloom ratios of p50 and p90, given the
    cell's spread of each. A difference inside the spread is `equal`, never a fail; for the equal
    classes (Q6, Q7) that is the pass condition. Dense classes check p50 only."""
    rule = gates["g7"][kind]
    inside = abs(r50 - 1) <= s50 and (kind == "dense" or abs(r90 - 1) <= s90)
    if kind == "equal":
        return PASS if inside else FAIL
    if inside:
        return EQUAL
    if kind == "dense":
        return PASS if r50 <= rule["p50_ratio_max"] else FAIL
    ok = r50 <= rule["p50_ratio_max"] and r90 <= rule["p90_ratio_max"]
    if width == sample.WINDOW or (isinstance(width, int) and width >= rule["wide_width_min"]):
        ok = ok and r50 <= rule["wide_p50_ratio_max"]
    return PASS if ok else FAIL


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
        elif a.digests != b.digests:
            row["verdict"], row["note"] = NOT_MEASURED, "results differ between the paths (G1)"
        else:
            row["p50_ratio"] = _ratio(a.p(0.5), b.p(0.5))
            row["p90_ratio"] = _ratio(a.p(0.9), b.p(0.9))
            row["verdict"] = g7_verdict(kind, width, row["p50_ratio"], row["p90_ratio"], s50, s90, gates)
        rows.append(row)
    return rows


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


def read_jsonl(path: str) -> list[dict[str, Any]]:
    with open(path) as file:
        return [json.loads(line) for line in file if line.strip()]


def read_csv(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def seq_cells(found: dict[str, Any] | None) -> Cells | None:
    return cells(read_jsonl(os.path.join(found["dir"], "seq.jsonl"))) if found else None


def build(root: str, gates: dict[str, Any]) -> tuple[str, dict[str, list[list[Any]]]]:
    """Returns `report.md` and the chart CSVs (name → rows, header first) of one commit."""
    report = _Report(load_runs(root), gates, os.path.basename(os.path.normpath(root)))
    for section in (report.g7, report.g8, report.g1, report.g4, report.sweeps, report.oneshot, report.index_build, report.summary):
        section()
    return "\n".join(report.md) + "\n", report.charts


class _Report:
    """The sections of one commit's report, written in order into `md` and `charts`."""

    def __init__(self, runs: list[dict[str, Any]], gates: dict[str, Any], commit: str) -> None:
        self.runs, self.gates, self.commit = runs, gates, commit
        self.floor = gates["spread_floor"]
        self.md = [f"# FilterMaps bench report: commit {commit}", "", f"Gates: `gates.json`, spread floor {self.floor:.0%}.", ""]
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
        """The per-cell spread of one cache state (D4): each spread run against the run of the same
        query file, mode and cache state, the larger of the two modes."""
        merged: dict[tuple[str, sample.Width], tuple[float, float]] = {}
        sources = []
        for mode in ("index", "bloom"):
            repeat = pick(self.runs, "run", mode, cache, spread=True, kind="g7")
            base = repeat and pick(self.runs, "run", mode, cache, kind="g7", queries=repeat["meta"]["queries"])
            if repeat and base:
                sources.append(mode)
                for key, (s50, s90) in spreads(seq_cells(base) or {}, seq_cells(repeat) or {}, self.floor).items():
                    old = merged.get(key, (0.0, 0.0))
                    merged[key] = (max(old[0], s50), max(old[1], s90))
        source = f"measured from the {' and '.join(sources)} spread runs" if sources else f"no spread run, so the floor ({self.floor:.0%}) applies to every cell"
        self.md += [f"Spread: {source}.", ""]
        return merged

    def g7(self) -> None:
        verdicts = []
        for cache in ("warm", "cold"):
            index, bloom = (seq_cells(pick(self.runs, "run", mode, cache, kind="g7")) for mode in ("index", "bloom"))
            if index is None and bloom is None:
                continue
            self.heading(f"G7 single-query latency, {cache}")
            rows = g7_rows(index or {}, bloom or {}, self.spread(cache), self.gates, seq_cells(pick(self.runs, "run", "today", cache, kind="g7")))
            verdicts += [row["verdict"] for row in rows]
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
        self.verdicts["G7"] = worst(verdicts)

    def g8(self) -> None:
        summaries: dict[str, dict[int, dict[str, Any]]] = {}
        for mode in ("index", "bloom", "today"):
            found = pick(self.runs, "load", mode)
            lines = read_jsonl(os.path.join(found["dir"], "load.jsonl")) if found else []
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
        self.md += ["A flagged client (above 80 % of a core) or any request that was not `ok` makes a row `not measured`. G8 also needs G4 under load.", ""]

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
        for phase, mode in (("off", "bloom"), ("catchup", "index"), ("load", "index")):
            found = pick(self.runs, "watch", mode, phase=phase)
            block_ms[phase] = [float(row["elapsed_ms"]) for row in read_csv(os.path.join(found["dir"], "blocks.csv"))] if found else []
        rows: list[list[Any]] = []
        for phase, values in block_ms.items():
            r50, r99, verdict = g4_verdict(block_ms["off"], values, self.gates) if phase != "off" else (None, None, "reference")
            rows.append([phase, len(values), _ms(stats.percentile(values, 0.5)), _ms(stats.percentile(values, 0.99)), _num(r50, 3), _num(r99, 3), verdict])
        self.verdicts["G4"] = worst([row[-1] for row in rows[1:]])
        self.heading("G4 node cost: block execution time")
        self.md += _table(["phase", "blocks", "p50 ms", "p99 ms", "p50 ratio", "p99 ratio", "verdict"], rows)
        limit = self.gates["g4"]["p50_ratio_max"]
        self.md += [f"Ratios against the index-off reference; the limit is {limit} × or 1 + the spread floor, whichever is larger.", ""]

    def sweeps(self) -> None:
        rows: list[list[Any]] = []
        found_runs = [r for r in self.runs if r["meta"]["command"] == "sweep" and r["meta"]["ended"]]
        for found in sorted(found_runs, key=lambda r: (r["meta"]["filter_id"], r["meta"]["chunk"], r["meta"]["mode"])):
            mode = found["meta"]["mode"]
            lines = [line for line in read_jsonl(os.path.join(found["dir"], "sweep.jsonl")) if line.get("summary")]
            for line in lines:
                work = [sum(line["counters"].get(f"reth_rpc_eth_filter_{name}_total", {}).values()) for name in ("headers_read", "receipt_sets_read")]
                values: list[Any] = [line["filter_id"], line["chunk"], mode, line["run"], line["requests"], line["total_ms"] / 1000,
                                     line["blocks_per_s"], line["requests_per_s"], line["logs"], *work, line["completed"]]
                self.charts["sweep.csv"].append(values)
                rows.append(values)
            if len(lines) > 1:
                total_s, blocks_per_s, requests_per_s = (sum(line[k] or 0 for line in lines) / len(lines) for k in ("total_ms", "blocks_per_s", "requests_per_s"))
                rows.append([lines[0]["filter_id"], lines[0]["chunk"], mode, "mean", lines[0]["requests"], total_s / 1000, blocks_per_s, requests_per_s,
                             None, None, None, all(line["completed"] for line in lines)])
        self.heading("Sweeps")
        self.md += _table(["filter", "chunk", "mode", "run", "requests", "total s", "blocks/s", "requests/s", "logs", "headers read", "receipt sets read", "completed"],
                          [[_num(v, 1) if isinstance(v, float) else v for v in row] for row in rows])
        self.md += ["The kit runs on the node's machine: a request has no network delay. A remote indexer adds one round trip per request to both paths.", ""]

    def oneshot(self) -> None:
        self.heading("One-shot over the whole window (D2)")
        shots: dict[str, dict[str, tuple[dict[str, Any], float]]] = {}
        for mode in ("index", "bloom"):
            found = pick(self.runs, "run", mode, kind="oneshot")
            if found is None:
                continue
            for line in read_jsonl(os.path.join(found["dir"], "seq.jsonl")):
                shots.setdefault(line["query_id"], {})[mode] = (line, found["meta"]["timeout_s"])
        for query_id, by_mode in sorted(shots.items()):
            parts = []
            for mode, (line, timeout_s) in sorted(by_mode.items()):
                if line["error"] == rpc.TIMEOUT:
                    parts.append(f"{mode} > {timeout_s / 60:.0f} min")
                else:
                    parts.append(f"{mode} " + (f"error {line['error']}" if line["error"] else f"{line['latency_ms']:.1f} ms"))
            self.md.append(f"- {query_id}: {', '.join(parts)}")
        self.md.append("")
        q1_id = next((query_id for query_id in sorted(shots) if query_id.startswith("Q1-")), None)
        bloom_width = self.gates["d2"]["bloom_width"]
        bloom_p50 = None
        found = pick(self.runs, "run", "bloom", kind="g7")
        if found and q1_id:
            # The one-shot holds the first Q1 filter of g7.jsonl: the same filter index.
            reference = f"Q1-w{bloom_width}-{q1_id.split('-')[2]}"
            lines = read_jsonl(os.path.join(found["dir"], "seq.jsonl"))
            bloom_p50 = stats.percentile([line["latency_ms"] for line in lines if line["query_id"] == reference and line["error"] is None], 0.5)
        index_line = shots.get(q1_id or "", {}).get("index", (None, None))[0]
        self.verdicts["D2"] = d2_check(index_line["latency_ms"] if index_line else None, index_line["error"] if index_line else None, bloom_p50)
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
    parser.add_argument("--gates", default=DEFAULT_GATES, help="the gate thresholds (default: the kit's gates.json)")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    with open(args.gates) as file:
        gates = json.load(file)
    text, charts = build(args.root, gates)
    with open(os.path.join(args.root, "report.md"), "w") as file:
        file.write(text)
    for name, rows in charts.items():
        with open(os.path.join(args.root, name), "w", newline="") as file:
            csv.writer(file).writerows(rows)
    print(os.path.join(args.root, "report.md"))
    return 0
