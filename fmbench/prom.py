"""Parser for the Prometheus text exposition format that Reth serves on `/metrics`.

A snapshot maps `(name, frozenset(labels))` to the sample value. The kit discovers series by prefix
(`reth_rpc_eth_filter_`, `reth_filter_maps_`), never by a fixed list, so a new metric shows up
without a code change.
"""

from __future__ import annotations

import math
import urllib.request

Key = tuple[str, frozenset[tuple[str, str]]]

RPC_FILTER_PREFIX = "reth_rpc_eth_filter_"
FILTER_MAPS_PREFIX = "reth_filter_maps_"


class Snapshot(dict[Key, float]):
    """One scrape: sample values by `(name, labels)`, plus the `# TYPE` of each metric family."""

    def __init__(self) -> None:
        super().__init__()
        self.types: dict[str, str] = {}

    def value(self, name: str, **labels: str) -> float | None:
        """Returns the sample of `name` with exactly `labels`, or `None` if the node has no such
        series (for example a metric of a feature that is off)."""
        return self.get((name, frozenset(labels.items())))


def parse(text: str) -> Snapshot:
    """Parses one `/metrics` response. Comment lines other than `# TYPE` are skipped."""
    snapshot = Snapshot()
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            parts = line.split(None, 3)
            if len(parts) == 4 and parts[1] == "TYPE":
                snapshot.types[parts[2]] = parts[3]
            continue
        name, labels, rest = _split_series(line)
        snapshot[(name, labels)] = _parse_value(rest.split()[0])
    return snapshot


def counters(snapshot: Snapshot, prefix: str) -> dict[Key, float]:
    """Returns the series of `snapshot` whose name starts with `prefix` and that only grow:
    counters, and the `_sum`, `_count` and `_bucket` series of summaries and histograms. Gauges
    and summary quantiles are left out: a difference of two of them means nothing."""
    out = {}
    for key, value in snapshot.items():
        name = key[0]
        if name.startswith(prefix) and _is_monotonic(snapshot.types, name):
            out[key] = value
    return out


def delta(before: dict[Key, float], after: dict[Key, float]) -> dict[Key, float]:
    """Returns `after - before` for every series of `after`. A series missing from `before` counts
    from 0: Reth registers some series on their first use."""
    return {key: value - before.get(key, 0) for key, value in after.items()}


def scrape(url: str, timeout_s: float = 30) -> Snapshot:
    """Fetches and parses `url` (the node's `--metrics` endpoint)."""
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return parse(response.read().decode())


def _is_monotonic(types: dict[str, str], name: str) -> bool:
    if types.get(name) == "counter":
        return True
    for suffix in ("_sum", "_count", "_bucket"):
        family = name[: -len(suffix)]
        if name.endswith(suffix) and types.get(family) in ("summary", "histogram"):
            return True
    return False


def _split_series(line: str) -> tuple[str, frozenset[tuple[str, str]], str]:
    """Splits a sample line into its name, its labels and the text after them."""
    brace = line.find("{")
    space = line.find(" ")
    if brace == -1 or (space != -1 and space < brace):
        return line[:space], frozenset(), line[space + 1 :]
    name = line[:brace]
    labels = []
    i = brace + 1
    while line[i] != "}":
        eq = line.index("=", i)
        key = line[i:eq].strip().lstrip(",").strip()
        if line[eq + 1] != '"':
            raise ValueError(f"unquoted label value in {line!r}")
        value, i = _read_quoted(line, eq + 2)
        labels.append((key, value))
        while line[i] in ", ":
            i += 1
    return name, frozenset(labels), line[i + 1 :]


def _read_quoted(line: str, i: int) -> tuple[str, int]:
    """Reads a label value that starts after its opening quote at `i`. Returns the unescaped value
    and the index after the closing quote."""
    out = []
    while line[i] != '"':
        if line[i] == "\\":
            i += 1
            out.append({"n": "\n", "\\": "\\", '"': '"'}.get(line[i], "\\" + line[i]))
        else:
            out.append(line[i])
        i += 1
    return "".join(out), i + 1


def _parse_value(text: str) -> float:
    if text in ("+Inf", "Inf"):
        return math.inf
    if text == "-Inf":
        return -math.inf
    value = float(text)
    return int(value) if value.is_integer() and "." not in text and "e" not in text.lower() else value
