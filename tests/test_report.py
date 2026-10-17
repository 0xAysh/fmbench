import json
import os
import unittest

from fmbench import report

with open(os.path.join(os.path.dirname(__file__), "..", "gates.json")) as file:
    GATES = json.load(file)


def seq(query_id, latencies, digest="d", error_rep=None):
    """`seq.jsonl` lines of one query, one per latency."""
    return [
        {
            "query_id": query_id,
            "rep": rep,
            "latency_ms": latency,
            "logs": None if rep == error_rep else 1,
            "digest": None if rep == error_rep else digest,
            "error": "timeout" if rep == error_rep else None,
            "counters": {"reth_rpc_eth_filter_headers_read_total": {"index": 2, "bloom": 0}},
        }
        for rep, latency in enumerate(latencies)
    ]


class G7Test(unittest.TestCase):
    def test_verdicts_on_a_canned_result_set(self):
        index = (
            seq("Q1-w10000-000", [10, 20, 30])  # selective, far below bloom: pass
            + seq("Q3-w500-000", [150, 160, 170])  # dense, 1.6 x bloom: fail
            + seq("Q2-w500-000", [102, 103, 104])  # selective, 3 % over bloom, spread 5 %: equal
            + seq("Q7-w500-000", [50, 50, 50], error_rep=1)  # an error: not measured
        )
        bloom = (
            seq("Q1-w10000-000", [400, 410, 420])
            + seq("Q3-w500-000", [90, 100, 110])
            + seq("Q2-w500-000", [99, 100, 101])
            + seq("Q7-w500-000", [50, 50, 50])
        )
        rows = report.g7_rows(report.cells(index), report.cells(bloom), {}, GATES)
        self.assertEqual(
            {(r["class"], r["width"]): r["verdict"] for r in rows},
            {
                ("Q1", 10000): "pass",
                ("Q2", 500): "equal",
                ("Q3", 500): "fail",
                ("Q7", 500): "not measured",
            },
        )

    def test_a_digest_difference_invalidates_the_cell(self):
        rows = report.g7_rows(
            report.cells(seq("Q1-w500-000", [10, 10], digest="a")),
            report.cells(seq("Q1-w500-000", [90, 90], digest="b")),
            {},
            GATES,
        )
        self.assertEqual(rows[0]["verdict"], "not measured")


class SpreadTest(unittest.TestCase):
    def test_is_the_relative_p50_and_p90_difference_of_two_runs_or_the_floor(self):
        a = report.cells(seq("Q1-w500-000", [100, 100, 200]) + seq("Q3-w500-000", [100, 100, 100]))
        b = report.cells(seq("Q1-w500-000", [110, 110, 220]) + seq("Q3-w500-000", [101, 101, 101]))
        spreads = report.spreads(a, b, floor=0.05)
        # Q1: p50 100 vs 110; p90 180 vs 198. Q3: 1 % apart, under the floor.
        self.assertAlmostEqual(spreads[("Q1", 500)][0], 0.10)
        self.assertAlmostEqual(spreads[("Q1", 500)][1], 0.10)
        self.assertEqual(report.spreads(a, b, floor=0.05)[("Q3", 500)], (0.05, 0.05))

    def test_a_ratio_inside_a_wider_spread_is_equal(self):
        self.assertEqual(report.g7_verdict("selective", 500, 0.93, 0.93, 0.08, 0.08, GATES), "equal")
        self.assertEqual(report.g7_verdict("selective", 500, 0.93, 0.93, 0.05, 0.05, GATES), "pass")


class WideRuleTest(unittest.TestCase):
    def test_selective_cells_at_wide_widths_need_half_the_bloom_p50(self):
        self.assertEqual(report.g7_verdict("selective", 10_000, 0.6, 0.6, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.6, 0.6, 0.05, 0.05, GATES), "fail")
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.45, 0.7, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("selective", "window", 0.6, 0.6, 0.05, 0.05, GATES), "fail")

    def test_dense_and_equal_rules(self):
        self.assertEqual(report.g7_verdict("dense", 100_000, 1.08, 2.0, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("dense", 500, 1.11, 1.0, 0.05, 0.05, GATES), "fail")
        self.assertEqual(report.g7_verdict("equal", 500, 1.04, 0.97, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("equal", 500, 1.04, 0.9, 0.05, 0.05, GATES), "fail")


class D2Test(unittest.TestCase):
    def test_the_index_whole_window_q1_must_not_cost_more_than_the_bloom_100k_p50(self):
        self.assertEqual(report.d2_check(800.0, None, 1200.0), "pass")
        self.assertEqual(report.d2_check(1200.0, None, 1200.0), "pass")
        self.assertEqual(report.d2_check(1500.0, None, 1200.0), "fail")
        self.assertEqual(report.d2_check(None, "timeout", 1200.0), "fail")
        self.assertEqual(report.d2_check(None, {"code": -32000}, 1200.0), "not measured")
        self.assertEqual(report.d2_check(800.0, None, None), "not measured")


def load_summary(qps, p99, statuses=None, cpu_flag=False):
    return {"qps": qps, "latency_ms": {"all": {"p99": p99}}, "statuses": statuses or {"ok": 100}, "client_cpu_flag": cpu_flag}


class G8Test(unittest.TestCase):
    def test_index_needs_at_least_the_bloom_qps_and_at_most_its_p99(self):
        self.assertEqual(report.g8_verdict(load_summary(12, 80), load_summary(10, 100), GATES), "pass")
        self.assertEqual(report.g8_verdict(load_summary(10.2, 101), load_summary(10, 100), GATES), "equal")
        self.assertEqual(report.g8_verdict(load_summary(8, 80), load_summary(10, 100), GATES), "fail")
        self.assertEqual(report.g8_verdict(load_summary(12, 80, cpu_flag=True), load_summary(10, 100), GATES), "not measured")
        self.assertEqual(report.g8_verdict(load_summary(12, 80), load_summary(10, 100, {"ok": 90, "timeout": 1}), GATES), "not measured")
        self.assertEqual(report.g8_verdict(None, load_summary(10, 100), GATES), "not measured")


class G4Test(unittest.TestCase):
    def test_block_p50_and_p99_may_grow_by_at_most_5_percent(self):
        reference = [10.0] * 95 + [50.0] * 5
        r50, r99, verdict = report.g4_verdict(reference, [10.4] * 95 + [52.0] * 5, GATES)
        self.assertAlmostEqual(r50, 1.04)
        self.assertAlmostEqual(r99, 1.04)
        self.assertEqual(verdict, "pass")
        self.assertEqual(report.g4_verdict(reference, [11.0] * 95 + [50.0] * 5, GATES)[2], "fail")
        self.assertEqual(report.g4_verdict(reference, [10.0] * 95 + [60.0] * 5, GATES)[2], "fail")
        self.assertEqual(report.g4_verdict([], [10.0], GATES)[2], "not measured")
