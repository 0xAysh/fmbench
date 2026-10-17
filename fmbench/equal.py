"""`equal`: the G1 comparison of two `seq.jsonl` files (index path vs bloom path)."""

from __future__ import annotations

import argparse
import json
from typing import Any


def first_reps(path: str) -> dict[str, dict[str, Any]]:
    """Returns the first repetition of each query of a `seq.jsonl` file."""
    lines: dict[str, dict[str, Any]] = {}
    with open(path) as file:
        for text in file:
            if text.strip():
                line = json.loads(text)
                if line["query_id"] not in lines or line["rep"] < lines[line["query_id"]]["rep"]:
                    lines[line["query_id"]] = line
    return lines


def compare(a: str, b: str) -> tuple[int, list[str]]:
    """Joins two runs on the query id (first repetition) and returns the number of queries and
    every difference: a different digest or log count, an error in either run, or a query that
    one run lacks."""
    runs = {"a": first_reps(a), "b": first_reps(b)}
    ids = sorted(runs["a"].keys() | runs["b"].keys())
    differences = []
    for query_id in ids:
        lines = {name: run.get(query_id) for name, run in runs.items()}
        problems = [f"missing in {name}" for name, line in lines.items() if line is None]
        problems += [f"error in {name}: {_text(line['error'])}" for name, line in lines.items() if line and line["error"] is not None]
        la, lb = lines["a"], lines["b"]
        if not problems and la is not None and lb is not None:
            for field in ("logs", "digest"):
                if la[field] != lb[field]:
                    problems.append(f"{field} {la[field]} != {lb[field]}")
        if problems:
            differences.append(f"{query_id}: {', '.join(problems)}")
    return len(ids), differences


def _text(error: Any) -> str:
    return error if isinstance(error, str) else json.dumps(error)


def add_parser(subparsers: Any) -> None:
    parser = subparsers.add_parser("equal", help="compare the digests of two seq.jsonl files (G1)")
    parser.add_argument("a", help="seq.jsonl of one run (index mode)")
    parser.add_argument("b", help="seq.jsonl of the other run (bloom mode)")
    parser.set_defaults(func=main)


def main(args: argparse.Namespace) -> int:
    compared, differences = compare(args.a, args.b)
    for difference in differences:
        print(difference)
    print(f"{compared} queries compared, {len(differences)} differences")
    return 1 if differences else 0
