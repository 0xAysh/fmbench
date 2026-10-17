import json
import os
import unittest

from fmbench import report, sample

with open(os.path.join(os.path.dirname(__file__), "..", "gates.json")) as file:
    GATES = json.load(file)


HEADERS = "reth_rpc_eth_filter_headers_read_total"
WINDOWS = "reth_rpc_eth_filter_windows_total"
FALLBACKS = "reth_rpc_eth_filter_fallbacks_total"


def found(started, mode="index", spread=False, used=100, reps=25):
    """A finished warm g7 run of one query file with `used` queries and `reps` repetitions."""
    meta = {"command": "run", "mode": mode, "cache": "warm", "kind": "g7", "ended": "x", "started": started,
            "spread": spread, "queries": {"sha256": "a"}, "queries_used": used, "reps": reps}
    return {"dir": started, "meta": meta}


def seq(query_id, latencies, digest="d", error_rep=None, counters=None):
    """`seq.jsonl` lines of one query, one per latency."""
    return [
        {
            "query_id": query_id,
            "rep": rep,
            "latency_ms": latency,
            "logs": None if rep == error_rep else 1,
            "digest": None if rep == error_rep else digest,
            "error": "timeout" if rep == error_rep else None,
            "counters": counters or {HEADERS: {"index": 2, "bloom": 0}},
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

    def test_an_index_request_the_header_bloom_served_in_part_is_not_index_latency(self):
        mixed = {WINDOWS: {"index": 3, "bloom": 1}, FALLBACKS: {"": 0}}
        failed = {WINDOWS: {"index": 0, "bloom": 4}, FALLBACKS: {"": 1}}
        index = seq("Q1-w500-000", [10, 10], counters=mixed) + seq("Q2-w500-000", [10, 10], counters=failed)
        bloom = seq("Q1-w500-000", [90, 90]) + seq("Q2-w500-000", [90, 90])
        rows = report.g7_rows(report.cells(index), report.cells(bloom), {}, GATES)
        self.assertEqual([r["verdict"] for r in rows], ["not measured", "not measured"])

    def test_the_wildcard_is_served_by_the_header_bloom_on_both_paths(self):
        wildcard = {WINDOWS: {"index": 0, "bloom": 1}, FALLBACKS: {"": 0}}
        rows = report.g7_rows(
            report.cells(seq("Q7-w100-000", [50, 50], counters=wildcard)), report.cells(seq("Q7-w100-000", [50, 50])), {}, GATES
        )
        self.assertEqual(rows[0]["verdict"], "pass")

    def test_lists_the_classes_without_a_cell(self):
        rows = report.g7_rows(report.cells(seq("Q1-w500-000", [10])), report.cells(seq("Q1-w500-000", [90])), {}, GATES)
        self.assertEqual(report.missing_classes(rows, GATES), ["Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9"])


class SpreadTest(unittest.TestCase):
    def test_is_the_relative_p50_and_p90_difference_of_two_runs_or_the_floor(self):
        a = report.cells(seq("Q1-w500-000", [100, 100, 200]) + seq("Q3-w500-000", [100, 100, 100]))
        b = report.cells(seq("Q1-w500-000", [110, 110, 220]) + seq("Q3-w500-000", [101, 101, 101]))
        spreads = report.spreads(a, b, floor=0.05)
        # Q1: p50 100 vs 110; p90 180 vs 198. Q3: 1 % apart, under the floor.
        self.assertAlmostEqual(spreads[("Q1", 500)][0], 0.10)
        self.assertAlmostEqual(spreads[("Q1", 500)][1], 0.10)
        self.assertEqual(report.spreads(a, b, floor=0.05)[("Q3", 500)], (0.05, 0.05))

    def test_pairs_the_spread_run_with_a_run_of_the_same_queries_and_repetitions(self):
        base, repeat = found("1"), found("4", spread=True)
        runs = [base, found("2", used=20), found("3", reps=1), repeat]
        self.assertEqual(report.spread_pair(runs, "index", "warm"), (base, repeat))
        self.assertIsNone(report.spread_pair([found("2", used=20), repeat], "index", "warm"))

    def test_a_ratio_inside_a_wider_spread_is_equal(self):
        self.assertEqual(report.g7_verdict("selective", 500, 0.93, 0.93, 0.08, 0.08, GATES), "equal")
        self.assertEqual(report.g7_verdict("selective", 500, 0.93, 0.93, 0.05, 0.05, GATES), "pass")


class WideRuleTest(unittest.TestCase):
    def test_selective_cells_at_wide_widths_need_half_the_bloom_p50(self):
        self.assertEqual(report.g7_verdict("selective", 10_000, 0.6, 0.6, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.6, 0.6, 0.05, 0.05, GATES), "fail")
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.45, 0.7, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("selective", "window", 0.6, 0.6, 0.05, 0.05, GATES), "fail")

    def test_each_ratio_is_judged_on_its_own(self):
        # p50 inside the spread (equal), p90 clearly lower (pass): equal, not a fail.
        self.assertEqual(report.g7_verdict("selective", 500, 1.03, 0.70, 0.05, 0.05, GATES), "equal")
        self.assertEqual(report.g7_verdict("selective", 500, 1.03, 1.20, 0.05, 0.05, GATES), "fail")

    def test_a_ratio_within_the_spread_of_a_wide_limit_is_equal(self):
        # The limit is 0.5: equal from 0.475 to 0.525.
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.51, 0.7, 0.05, 0.05, GATES), "equal")
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.97, 0.7, 0.05, 0.05, GATES), "fail")

    def test_dense_and_equal_rules(self):
        # The dense limit is 1.10: equal from 1.045 to 1.155.
        self.assertEqual(report.g7_verdict("dense", 100_000, 1.0, 2.0, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("dense", 500, 1.12, 1.0, 0.05, 0.05, GATES), "equal")
        self.assertEqual(report.g7_verdict("dense", 500, 1.2, 1.0, 0.05, 0.05, GATES), "fail")
        self.assertEqual(report.g7_verdict("equal", 500, 1.04, 0.97, 0.05, 0.05, GATES), "pass")
        self.assertEqual(report.g7_verdict("equal", 500, 1.04, 0.9, 0.05, 0.05, GATES), "fail")


class BandTest(unittest.TestCase):
    def test_a_worse_ratio_never_gets_a_better_verdict(self):
        cases = {
            "selective at 500": lambda r: report.g7_verdict("selective", 500, r, 0.5, 0.05, 0.05, GATES),
            "selective at 100,000": lambda r: report.g7_verdict("selective", 100_000, r, 0.4, 0.05, 0.05, GATES),
            "dense": lambda r: report.g7_verdict("dense", 500, r, 1.0, 0.05, 0.05, GATES),
            "G8 p99": lambda r: report.g8_verdict(load_summary(10, 100 * r), load_summary(10, 100), GATES),
            "G8 QPS": lambda r: report.g8_verdict(load_summary(10 / r, 50), load_summary(10, 100), GATES),
        }
        for name, verdict in cases.items():
            with self.subTest(name):
                # SEVERITY runs from the worst verdict to the best.
                ranks = [report.SEVERITY.index(verdict(r / 100)) for r in range(30, 160)]
                self.assertEqual(ranks, sorted(ranks, reverse=True))


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

    def test_each_ratio_is_judged_on_its_own(self):
        # QPS inside the floor (equal), p99 halved (pass): equal, not a fail.
        self.assertEqual(report.g8_verdict(load_summary(9.8, 50), load_summary(10, 100), GATES), "equal")

    def test_no_bloom_answer_or_no_latency_is_not_measured(self):
        self.assertEqual(report.g8_verdict(load_summary(5, 80), load_summary(0, 100), GATES), "not measured")
        self.assertEqual(report.g8_verdict(load_summary(5, None), load_summary(5, 100), GATES), "not measured")


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

    def test_load_is_compared_with_the_same_load_with_the_index_off(self):
        idle, loaded = [10.0] * 100, [20.0] * 100
        rows = report.g4_rows({("off", "bloom"): idle, ("catchup", "index"): [10.2] * 100, ("load", "bloom"): loaded, ("load", "index"): [20.4] * 100}, GATES)
        self.assertEqual([(r[0], r[1], r[-1]) for r in rows], [
            ("off", "bloom", "reference"), ("catchup", "index", "pass"), ("load", "bloom", "reference"), ("load", "index", "pass"),
        ])
        rows = report.g4_rows({("off", "bloom"): idle, ("catchup", "index"): [10.2] * 100, ("load", "bloom"): [], ("load", "index"): [20.4] * 100}, GATES)
        self.assertEqual(rows[-1][-1], "not measured")


def summary(run, completed=True, total_ms=1000.0, blocks_per_s=100.0, requests_per_s=2.0):
    return {"run": run, "completed": completed, "total_ms": total_ms, "blocks_per_s": blocks_per_s, "requests_per_s": requests_per_s}


class SweepMeanTest(unittest.TestCase):
    def test_is_the_mean_of_the_completed_runs(self):
        means = report.sweep_mean([summary(1), summary(2, total_ms=3000.0, blocks_per_s=50.0, requests_per_s=1.0), summary(3, False, 10.0, None, None)])
        self.assertEqual(means, {"completed": 2, "total_ms": 2000.0, "blocks_per_s": 75.0, "requests_per_s": 1.5})

    def test_needs_two_completed_runs(self):
        self.assertIsNone(report.sweep_mean([summary(1), summary(2, completed=False)]))


class FellBackTest(unittest.TestCase):
    def test_a_failed_lookup_or_any_bloom_window(self):
        self.assertFalse(report.fell_back("Q1", {WINDOWS: {"index": 3, "bloom": 0}, FALLBACKS: {"": 0}}))
        self.assertFalse(report.fell_back("Q1", {}))
        self.assertTrue(report.fell_back("Q1", {WINDOWS: {"index": 3, "bloom": 1}, FALLBACKS: {"": 0}}))
        self.assertTrue(report.fell_back("Q1", {WINDOWS: {"index": 0, "bloom": 2}, FALLBACKS: {"": 1}}))
        # A wiped store has no indexed range: every window goes to the bloom without a failed lookup.
        self.assertTrue(report.fell_back("Q1", {WINDOWS: {"index": 0, "bloom": 2}, FALLBACKS: {"": 0}}))

    def test_the_wildcard_is_never_looked_up(self):
        self.assertFalse(report.fell_back(sample.WILDCARD, {WINDOWS: {"index": 0, "bloom": 2}, FALLBACKS: {"": 0}}))


class G7RunsTest(unittest.TestCase):
    def test_compares_the_latest_index_run_with_runs_of_the_same_queries_and_repetitions(self):
        matrix = (found("1"), found("2", mode="bloom"), found("3", mode="today"))
        smoke = found("4", mode="bloom", used=20, reps=1)
        self.assertEqual(report.g7_runs([*matrix, smoke], "warm"), matrix)
        self.assertEqual(report.g7_runs([found("1"), smoke], "warm"), (found("1"), None, None))
        self.assertEqual(report.g7_runs([smoke], "warm"), (None, None, None))
