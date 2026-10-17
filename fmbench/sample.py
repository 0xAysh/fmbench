"""`sample`: builds the query files from real logs of the indexed window.

It writes three JSONL files, each with a header line: `g7.jsonl` (per class, up to five filters ×
every width), `g1.jsonl` (random queries for the equality gate) and `oneshot.jsonl` (the first Q1
and Q9 filters over the whole window, the D2 condition).
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, NamedTuple, Protocol, Union

from fmbench import meta, prom, rpc

# A width is a block count or "window", the whole indexed window.
Width = Union[int, str]

WINDOW = "window"

CLASSES = ("Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9")

# `Transfer(address,address,uint256)`, shared by ERC-20 and ERC-721.
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# `Approval(address,address,uint256)`.
APPROVAL = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

# Q6 (near the head) uses its own widths; the matrix widths do not apply to it.
HEAD_WIDTHS: tuple[Width, ...] = (1, 10, 100)
# A rare filter (Q1, Q9) has this many logs over the whole window.
RARE_LOGS = range(1, 101)
# The pool that the popular classes (Q3, Q5) draw their filters from.
POPULAR_POOL = 10
DEFAULT_WIDTHS = "500,2000,10000,100000"
# The sampled window starts this many blocks above the first indexed block. The index drops whole
# epochs (about 20,000 mainnet blocks) from its tail as the chain grows, so without a margin the
# window's first blocks leave the index within hours and `run` refuses the file. 100,000 blocks
# is about 14 days of chain growth.
DEFAULT_TAIL_MARGIN = 100_000
# The address counts of the Q8 filters.
MANY_ADDRESSES = (50, 200, 1000)
# The most whole-window `eth_getLogs` checks per rare class. Rare candidates pass often (an
# address seen once in a 2,000-block sample has about 1,000 logs in the window on average), so
# this only bounds a sample that has none.
MAX_RARE_CHECKS = 1000


@dataclass
class Tally:
    """Log counts over the sampled blocks. Hex is lowercased."""

    blocks: int = 0
    logs: int = 0
    address: Counter[str] = field(default_factory=Counter)
    topic0: Counter[str] = field(default_factory=Counter)
    pair: Counter[tuple[str, str]] = field(default_factory=Counter)
    # `Transfer` `topic1` (the sender) and `topic2` (the recipient).
    sender: Counter[str] = field(default_factory=Counter)
    recipient: Counter[str] = field(default_factory=Counter)
    # The distinct addresses that emitted each `topic0`.
    emitters: dict[str, set[str]] = field(default_factory=dict)
    # The `topic0` of the last log seen from each address: for an address seen once, its only one.
    topic0_of: dict[str, str] = field(default_factory=dict)


class Node(Protocol):
    """The JSON-RPC calls `build` needs; `rpc.Client` in a run, a fake in tests."""

    def call(self, method: str, params: list[Any]) -> Any: ...


class Pick(NamedTuple):
    """One filter of a class, without its block range."""

    filter: dict[str, Any]
    # Matching logs per sampled block: `expected_logs` is this rate × the width.
    rate: float
    # The logs over the whole window, for the filters whose selection counted them.
    window_logs: int | None = None


@dataclass
class Selection:
    """The filters of each class, and what the header records about the choice."""

    filters: dict[str, list[Pick]]
    notes: dict[str, Any]


def pick_filters(
    tally: Tally,
    window_logs: Callable[[dict[str, Any]], int],
    rng: random.Random,
    per_class: int = 5,
) -> Selection:
    """Picks up to `per_class` filters per class from the sample's tallies (bench-kit spec K2).

    `window_logs` counts a filter's logs over the whole indexed window (one `eth_getLogs`).
    """
    filters: dict[str, list[Pick]] = {}
    notes: dict[str, Any] = {}
    rate = 1 / tally.blocks

    singletons = sorted(a for a, n in tally.address.items() if n == 1 and a in tally.topic0_of)
    filters["Q1"] = _rare(
        [{"address": [a], "topics": []} for a in singletons], window_logs, rng, per_class, rate
    )
    filters["Q2"] = [
        Pick({"address": p.filter["address"], "topics": [tally.topic0_of[p.filter["address"][0]]]}, rate)
        for p in filters["Q1"]
    ]

    emitters = _ranked(tally.address)[:POPULAR_POOL]
    filters["Q3"] = [
        Pick({"address": [a], "topics": []}, tally.address[a] * rate)
        for a in _draw(emitters, per_class, rng)
    ]

    top_topic0 = _ranked(tally.topic0)[0]
    filters["Q4"] = [Pick({"topics": [top_topic0]}, tally.topic0[top_topic0] * rate)]
    notes["q4_topic0"] = top_topic0
    notes["q4_is_transfer"] = top_topic0 == TRANSFER

    pairs = _ranked(tally.pair)[:POPULAR_POOL]
    filters["Q5"] = [
        Pick({"address": [a], "topics": [t]}, tally.pair[(a, t)] * rate)
        for a, t in _draw(pairs, per_class, rng)
    ]

    filters["Q8"], notes["q8_topic0"], notes["q8_addresses"] = _many_addresses(tally, rate)

    recipients = sorted(t for t, n in tally.recipient.items() if n == 1 and _is_address(t))
    filters["Q9"] = _rare(
        [{"topics": [TRANSFER, None, t]} for t in recipients], window_logs, rng, per_class, rate
    )
    if len(filters["Q9"]) < per_class:
        senders = sorted(t for t, n in tally.sender.items() if n == 1 and _is_address(t))
        filters["Q9"] += _rare(
            [{"topics": [TRANSFER, t]} for t in senders],
            window_logs,
            rng,
            per_class - len(filters["Q9"]),
            rate,
        )

    # Q6 alternates the rare and the popular filters, so the head check covers both shapes.
    head = [p for pair in itertools.zip_longest(filters["Q1"], filters["Q3"]) for p in pair if p]
    filters["Q6"] = head[:per_class]
    filters["Q7"] = [Pick({"topics": []}, tally.logs * rate)]
    return Selection({c: filters[c] for c in CLASSES}, notes)


def _many_addresses(tally: Tally, rate: float) -> tuple[list[Pick], str | None, list[int]]:
    """Returns the Q8 filters, their `topic0` and their address counts: the `topic0` other than
    `Transfer` and `Approval` with the most distinct emitters (in practice a factory event such as
    a pool's `Swap`), with its emitters ranked by sample count, at 50, 200 and 1,000 addresses or
    as many as the sample has."""
    candidates = [t for t in tally.emitters if t not in (TRANSFER, APPROVAL)]
    if not candidates:
        return [], None, []
    factory = min(candidates, key=lambda t: (-len(tally.emitters[t]), t))
    ranked = sorted(tally.emitters[factory], key=lambda a: (-tally.pair[(a, factory)], a))
    sizes = sorted({min(n, len(ranked)) for n in MANY_ADDRESSES})
    picks = [
        Pick(
            {"address": ranked[:n], "topics": [factory]},
            sum(tally.pair[(a, factory)] for a in ranked[:n]) * rate,
        )
        for n in sizes
    ]
    return picks, factory, sizes


def _is_address(topic: str) -> bool:
    """Whether a 32-byte topic holds an address: its first 12 bytes are zero."""
    return len(topic) == 66 and topic[2:26] == "0" * 24


def _ranked(counts: Counter[Any]) -> list[Any]:
    """Returns the keys by count, highest first, ties in key order."""
    return sorted(counts, key=lambda k: (-counts[k], k))


def _draw(ranked: list[Any], n: int, rng: random.Random) -> list[Any]:
    """Draws `n` of `ranked` (all of them if fewer), seeded, and keeps their rank order."""
    chosen = set(rng.sample(range(len(ranked)), min(n, len(ranked))))
    return [item for i, item in enumerate(ranked) if i in chosen]


def _rare(
    candidates: list[dict[str, Any]],
    window_logs: Callable[[dict[str, Any]], int],
    rng: random.Random,
    per_class: int,
    rate: float,
) -> list[Pick]:
    """Returns the first `per_class` candidates, in a seeded order, with 1–100 logs in the window."""
    rng.shuffle(candidates)
    picks = []
    for candidate in candidates[:MAX_RARE_CHECKS]:
        count = window_logs(candidate)
        if count in RARE_LOGS:
            picks.append(Pick(candidate, rate, count))
            if len(picks) == per_class:
                break
    return picks


def tally(blocks: Iterable[list[dict[str, Any]]]) -> Tally:
    """Counts the logs of each block's receipts (`eth_getBlockReceipts` results)."""
    out = Tally()
    for receipts in blocks:
        out.blocks += 1
        for receipt in receipts:
            for log in receipt["logs"]:
                address = log["address"].lower()
                topics = [topic.lower() for topic in log["topics"]]
                out.logs += 1
                out.address[address] += 1
                if not topics:
                    continue
                out.topic0[topics[0]] += 1
                out.pair[(address, topics[0])] += 1
                out.emitters.setdefault(topics[0], set()).add(address)
                out.topic0_of[address] = topics[0]
                if topics[0] == TRANSFER and len(topics) >= 3:
                    out.sender[topics[1]] += 1
                    out.recipient[topics[2]] += 1
    return out


def parse_widths(text: str) -> list[Width]:
    """Parses `--widths`: comma-separated positive block counts, or `window`."""
    widths: list[Width] = []
    for part in text.split(","):
        if part == WINDOW:
            widths.append(WINDOW)
        elif part.isdigit() and int(part) > 0:
            widths.append(int(part))
        else:
            raise ValueError(f"invalid width {part!r}: expected a positive block count or {WINDOW!r}")
    return widths


def g7_queries(
    selection: Selection, widths: list[Width], window: tuple[int, int], max_logs: int
) -> list[dict[str, Any]]:
    """Returns every filter of every class at every width, each query ending at the window's last
    block. A query whose `expected_logs` is above `max_logs` is dropped (the result cap)."""
    out = []
    for cls, picks in selection.filters.items():
        for width in _widths_of(cls, widths):
            blocks = _blocks(width, window)
            for i, pick in enumerate(picks):
                expected = round(pick.rate * blocks)
                if expected <= max_logs:
                    out.append(_query(f"{cls}-w{width}-{i:03d}", cls, width, expected, pick, window[1] - blocks + 1, window[1]))
    return out


def oneshot_queries(selection: Selection, window: tuple[int, int]) -> list[dict[str, Any]]:
    """Returns the first Q1 and the first Q9 filter over the whole window: the D2 condition. The
    result cap does not apply; `expected_logs` is the count the selection measured."""
    return [
        _query(f"{cls}-w{WINDOW}-000", cls, WINDOW, pick.window_logs or 0, pick, *window)
        for cls in ("Q1", "Q9")
        for pick in selection.filters.get(cls, [])[:1]
    ]


def g1_queries(
    selection: Selection,
    widths: list[Width],
    window: tuple[int, int],
    count: int,
    max_logs: int,
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Returns `count` random queries for G1: a random class, then a random filter and width of
    that class under the result cap, at a random position inside the window."""
    cells: dict[str, list[tuple[int, Width, int]]] = {}
    for cls, picks in selection.filters.items():
        for width in _widths_of(cls, widths):
            blocks = _blocks(width, window)
            for i, pick in enumerate(picks):
                if round(pick.rate * blocks) <= max_logs:
                    cells.setdefault(cls, []).append((i, width, blocks))
    classes = sorted(cells)
    out = []
    for n in range(count):
        cls = rng.choice(classes)
        i, width, blocks = rng.choice(cells[cls])
        pick = selection.filters[cls][i]
        first = rng.randint(window[0], window[1] - blocks + 1)
        out.append(
            _query(f"{cls}-w{width}-{i:03d}-{n:05d}", cls, width, round(pick.rate * blocks), pick, first, first + blocks - 1)
        )
    return out


def query_cell(query_id: str) -> tuple[str, Width]:
    """Returns the class and width of a query id (`Q1-w100000-003`, `Q6-w10-001-00042`)."""
    cls, width = query_id.split("-")[:2]
    return cls, WINDOW if width[1:] == WINDOW else int(width[1:])


def _widths_of(cls: str, widths: list[Width]) -> list[Width]:
    return list(HEAD_WIDTHS) if cls == "Q6" else widths


def _blocks(width: Width, window: tuple[int, int]) -> int:
    """Returns the block count of `width` in `window`."""
    size = window[1] - window[0] + 1
    if width == WINDOW:
        return size
    assert isinstance(width, int)
    if width > size:
        raise ValueError(f"width {width} is wider than the indexed window ({size} blocks); use {WINDOW!r}")
    return width


def _query(
    query_id: str, cls: str, width: Width, expected: int, pick: Pick, from_block: int, to_block: int
) -> dict[str, Any]:
    return {
        "id": query_id,
        "class": cls,
        "width": width,
        "expected_logs": expected,
        "filter": {"fromBlock": hex(from_block), "toBlock": hex(to_block), **pick.filter},
    }


def build(
    node: Node,
    window: tuple[int, int],
    seed: int,
    sampled_blocks: int,
    widths: list[Width],
    count: int,
    max_logs: int,
    node_commit: str,
    progress: Callable[[str], None] = lambda _: None,
) -> dict[str, tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Samples the window and returns the header and queries of `g7`, `g1` and `oneshot`.

    One seeded generator drives every random choice, so the same seed over the same chain gives
    the same files.
    """
    first, last = window
    rng = random.Random(seed)
    numbers = sorted(rng.sample(range(first, last + 1), min(sampled_blocks, last - first + 1)))

    def receipts() -> Iterable[list[dict[str, Any]]]:
        for done, number in enumerate(numbers, 1):
            if done % 200 == 0:
                progress(f"sampled {done}/{len(numbers)} blocks")
            yield node.call("eth_getBlockReceipts", [hex(number)])

    def window_logs(filter_: dict[str, Any]) -> int:
        return len(node.call("eth_getLogs", [{"fromBlock": hex(first), "toBlock": hex(last), **filter_}]))

    selection = pick_filters(tally(receipts()), window_logs, rng)
    header = {
        "window": [first, last],
        "to_block": last,
        "seed": seed,
        "sampled_blocks": len(numbers),
        "widths": widths,
        "node_commit": node_commit,
        "max_logs": max_logs,
        "classes": {cls: len(picks) for cls, picks in selection.filters.items()},
        **selection.notes,
    }
    return {
        "g7": ({**header, "kind": "g7"}, g7_queries(selection, widths, window, max_logs)),
        "g1": ({**header, "kind": "g1", "count": count}, g1_queries(selection, widths, window, count, max_logs, rng)),
        "oneshot": ({**header, "kind": "oneshot", "widths": [WINDOW]}, oneshot_queries(selection, window)),
    }


def format_queries(header: dict[str, Any], queries: list[dict[str, Any]]) -> str:
    """Returns a query file: the header line, then one line per query."""
    lines = [json.dumps({"header": header})] + [json.dumps(query) for query in queries]
    return "\n".join(lines) + "\n"


def read_queries(path: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Reads a query file written by `sample`."""
    with open(path) as file:
        lines = [json.loads(line) for line in file if line.strip()]
    if not lines or "header" not in lines[0]:
        raise ValueError(f"{path}: not a query file (no header line)")
    return lines[0]["header"], lines[1:]


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("sample", help="build g7.jsonl, g1.jsonl and oneshot.jsonl from the indexed window")
    meta.add_node_args(parser)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--blocks", type=int, default=2000, help="random blocks whose receipts are tallied")
    parser.add_argument("--widths", type=parse_widths, default=parse_widths(DEFAULT_WIDTHS), help=f"block counts or 'window' (default {DEFAULT_WIDTHS})")
    parser.add_argument("--count", type=int, default=10_000, help="queries in g1.jsonl")
    parser.add_argument("--max-logs", type=int, default=200_000, help="drop a query whose estimated logs are above this")
    parser.add_argument("--tail-margin", type=int, default=DEFAULT_TAIL_MARGIN, help="blocks between the first indexed block and the window start")
    parser.add_argument("--out", default="queries", help="directory of the query files")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    node = meta.node(args.unit, args.pid)
    meta.check_mode("index", node["cmdline"])
    snapshot = prom.scrape(args.metrics)
    first = snapshot.value("reth_filter_maps_first_indexed_block")
    last = snapshot.value("reth_filter_maps_last_indexed_block")
    if not first or not last:
        raise SystemExit("the node reports no indexed window: run sample against the node in index mode once it is indexed")
    window = (int(first) + args.tail_margin, int(last))
    if window[0] > window[1]:
        raise SystemExit(f"the indexed window {int(first)}..{int(last)} is shorter than --tail-margin {args.tail_margin}")
    paths = {kind: os.path.join(args.out, f"{kind}.jsonl") for kind in ("g7", "g1", "oneshot")}
    existing = [path for path in paths.values() if os.path.exists(path)]
    if existing:
        raise SystemExit(f"refusing to overwrite {', '.join(existing)}: results refer to them; use another --out")

    def progress(message: str) -> None:
        print(message, file=sys.stderr)

    built = build(rpc.Client(args.rpc), window, args.seed, args.blocks, args.widths, args.count, args.max_logs, node["commit"], progress)
    os.makedirs(args.out, exist_ok=True)
    for kind, (header, queries) in built.items():
        header["indexed_window"] = [int(first), int(last)]
        with open(paths[kind], "w") as file:
            file.write(format_queries(header, queries))
        print(f"{paths[kind]}: {len(queries)} queries")
    header = built["g7"][0]
    print(f"window {window[0]}..{window[1]}, classes {header['classes']}, Q4 topic0 {header['q4_topic0']} (Transfer: {header['q4_is_transfer']}), Q8 addresses {header['q8_addresses']}")
    return 0
