"""`python3 -m fmbench <command>`: the bench kit's single entry point."""

from __future__ import annotations

import argparse
import sys

from fmbench import cold, meta, run, sample, sweep


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fmbench", description=__doc__)
    subparsers = parser.add_subparsers(required=True, metavar="command")
    for module in (sample, run, cold, sweep):
        module.add_parser(subparsers)
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except meta.MetaError as error:
        print(f"fmbench: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
