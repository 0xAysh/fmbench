"""`python3 -m fmbench <command>`: the bench kit's single entry point."""

from __future__ import annotations

import argparse
import sys

from fmbench import cold, equal, load, meta, report, rpc, run, sample, sweep, watch


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="fmbench", description=__doc__)
    subparsers = parser.add_subparsers(required=True, metavar="command")
    for module in (sample, run, load, equal, cold, watch, sweep, report):
        module.add_parser(subparsers)
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (meta.MetaError, cold.ColdError, rpc.RpcError) as error:
        print(f"fmbench: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
