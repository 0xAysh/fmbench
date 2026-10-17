"""Percentiles, shared by `load`, `watch` and `report`."""

from __future__ import annotations

from collections.abc import Iterable


def percentile(values: Iterable[float], q: float) -> float | None:
    """Returns the `q`-quantile (0 ≤ q ≤ 1) by linear interpolation between the closest ranks
    (numpy's default), or `None` for no values."""
    ordered = sorted(values)
    if not ordered:
        return None
    rank = (len(ordered) - 1) * q
    low = int(rank)
    if low + 1 >= len(ordered):
        return ordered[-1]
    return ordered[low] + (rank - low) * (ordered[low + 1] - ordered[low])
