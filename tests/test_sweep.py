import unittest

from fmbench import sweep


class ChunksTest(unittest.TestCase):
    def test_splits_a_range_into_fixed_chunks_with_a_short_last_one(self):
        self.assertEqual(
            sweep.chunks(1_000_000, 1_024_999, 10_000),
            [(1_000_000, 1_009_999), (1_010_000, 1_019_999), (1_020_000, 1_024_999)],
        )

    def test_a_divisible_range_has_no_short_chunk(self):
        self.assertEqual(sweep.chunks(1, 20_000, 10_000), [(1, 10_000), (10_001, 20_000)])

    def test_one_block_and_a_chunk_wider_than_the_range(self):
        self.assertEqual(sweep.chunks(7, 7, 500), [(7, 7)])
        self.assertEqual(sweep.chunks(100, 150, 500), [(100, 150)])

    def test_the_request_count_over_the_window(self):
        window = (23_800_000, 26_149_999)
        self.assertEqual(len(sweep.chunks(*window, 500)), 4_700)
        self.assertEqual(len(sweep.chunks(*window, 2_000)), 1_175)
        self.assertEqual(len(sweep.chunks(*window, 10_000)), 235)
        self.assertEqual(len(sweep.chunks(*window, 100_000)), 24)


def chunk(run, first, last, latency_ms, logs, error=None, headers=0):
    return {
        "run": run,
        "chunk_from": first,
        "chunk_to": last,
        "latency_ms": latency_ms,
        "bytes": 100,
        "logs": None if error else logs,
        "digest": None if error else "d",
        "error": error,
        "counters": {"reth_rpc_eth_filter_headers_read_total": {"index": headers, "bloom": 0}},
    }


class SummaryTest(unittest.TestCase):
    def test_sums_a_complete_run(self):
        lines = [
            chunk(1, 1, 10_000, 400.0, 3, headers=2),
            chunk(1, 10_001, 20_000, 500.0, 0, headers=1),
            chunk(1, 20_001, 25_000, 100.0, 1, headers=1),
        ]
        self.assertEqual(
            sweep.summarize(lines, "Q1-w100000-003", 10_000, 1, planned=3),
            {
                "summary": True,
                "filter_id": "Q1-w100000-003",
                "chunk": 10_000,
                "run": 1,
                "requests": 3,
                "planned_requests": 3,
                "blocks": 25_000,
                "total_ms": 1000.0,
                "blocks_per_s": 25_000.0,
                "requests_per_s": 3.0,
                "logs": 4,
                "counters": {"reth_rpc_eth_filter_headers_read_total": {"index": 4, "bloom": 0}},
                "completed": True,
                "reached": 25_000,
                "error": None,
            },
        )

    def test_an_error_ends_the_run_as_incomplete_with_the_chunk_reached(self):
        lines = [chunk(2, 1, 10_000, 400.0, 3), chunk(2, 10_001, 20_000, 1_800_000.0, 0, error="timeout")]
        summary = sweep.summarize(lines, "Q1-w100000-003", 10_000, 2, planned=3)
        self.assertEqual(
            {k: summary[k] for k in ("requests", "planned_requests", "blocks", "completed", "reached", "error")},
            {"requests": 2, "planned_requests": 3, "blocks": 10_000, "completed": False, "reached": 10_000, "error": "timeout"},
        )
