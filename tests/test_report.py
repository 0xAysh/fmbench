import json
import os
import tempfile
import unittest

from fmbench import report, sample

with open(os.path.join(os.path.dirname(__file__), "..", "gates.json")) as file:
    GATES = json.load(file)


HEADERS = "reth_rpc_eth_filter_headers_read_total"
WINDOWS = "reth_rpc_eth_filter_windows_total"
FALLBACKS = "reth_rpc_eth_filter_fallbacks_total"


def found(started, mode="index", spread=False, used=100, reps=25, kind="g7", command="run", flags=("node",), **extra):
    """A finished warm run of one query file with `used` queries and `reps` repetitions, on a node
    with `flags` (its `node_flags`)."""
    meta = {"command": command, "mode": mode, "cache": "warm", "kind": kind, "ended": "x", "started": started, "spread": spread,
            "queries": {"file": f"{kind}.jsonl", "sha256": "a"}, "queries_used": used, "reps": reps, "node_flags": list(flags), **extra}
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

    def test_a_query_near_the_head_passes_when_the_index_is_faster(self):
        # Q6 takes the selective rule: faster passes, within the spread is equal.
        index = seq("Q6-w10-000", [10, 10]) + seq("Q6-w100-000", [103, 103])
        bloom = seq("Q6-w10-000", [50, 50]) + seq("Q6-w100-000", [100, 100])
        rows = report.g7_rows(report.cells(index), report.cells(bloom), {}, GATES)
        self.assertEqual([r["verdict"] for r in rows], ["pass", "equal"])

    def test_the_popular_filters_near_the_head_take_the_dense_rule(self):
        # 7 % slower: inside the dense band (1.048 to 1.155), above the selective one.
        index = seq("Q6-w100-000", [107, 107]) + seq("Q6d-w100-000", [107, 107])
        bloom = seq("Q6-w100-000", [100, 100]) + seq("Q6d-w100-000", [100, 100])
        rows = report.g7_rows(report.cells(index), report.cells(bloom), {}, GATES)
        self.assertEqual([(r["class"], r["verdict"]) for r in rows], [("Q6", "fail"), ("Q6d", "equal")])

    def test_lists_the_classes_without_a_cell(self):
        rows = report.g7_rows(report.cells(seq("Q1-w500-000", [10])), report.cells(seq("Q1-w500-000", [90])), {}, GATES)
        self.assertEqual(report.missing_classes(rows, GATES), ["Q2", "Q3", "Q4", "Q5", "Q6", "Q6d", "Q7", "Q8", "Q9"])


class RepetitionsTest(unittest.TestCase):
    def test_a_g7_run_under_5_repetitions_gets_no_verdict(self):
        with tempfile.TemporaryDirectory() as root:
            for run, latency in ((found("1", used=1, reps=1), 10), (found("2", mode="bloom", used=1, reps=1), 90)):
                os.makedirs(os.path.join(root, run["dir"]))
                with open(os.path.join(root, run["dir"], "meta.json"), "w") as file:
                    json.dump(run["meta"], file)
                with open(os.path.join(root, run["dir"], "seq.jsonl"), "w") as file:
                    file.writelines(json.dumps(line) + "\n" for line in seq("Q1-w500-000", [latency]))
            md, _ = report.build(root, GATES)
        self.assertIn("The index run `1`: `g7.jsonl`, 1 queries × 1 repetitions, against the bloom and today runs of the same queries and node flags.", md)
        self.assertIn("| not measured | 1 of the 5 repetitions a verdict needs |", md)
        self.assertIn("| G7 | not measured |", md)


class SpreadTest(unittest.TestCase):
    def test_is_the_relative_p50_and_p90_difference_of_two_runs_or_the_floor(self):
        a = report.cells(seq("Q1-w500-000", [100, 100, 200]) + seq("Q3-w500-000", [100, 100, 100]))
        b = report.cells(seq("Q1-w500-000", [110, 110, 220]) + seq("Q3-w500-000", [101, 101, 101]))
        spreads = report.spreads(a, b, floor=0.05)
        # Q1: p50 100 vs 110; p90 180 vs 198. Q3: 1 % apart, under the floor.
        self.assertAlmostEqual(spreads[("Q1", 500)][0], 0.10)
        self.assertAlmostEqual(spreads[("Q1", 500)][1], 0.10)
        self.assertEqual(report.spreads(a, b, floor=0.05)[("Q3", 500)], (0.05, 0.05))

    def test_pairs_the_spread_runs_of_the_judged_queries_and_repetitions(self):
        base, repeat = found("1"), found("2", spread=True)
        smoke, smoke_repeat = found("3", used=20, reps=1), found("4", spread=True, used=20, reps=1)
        runs = [base, repeat, smoke, smoke_repeat]
        self.assertEqual(report.spread_pair(runs, "index", "warm", base), (base, repeat))
        self.assertIsNone(report.spread_pair([base, smoke_repeat], "index", "warm", base))
        self.assertIsNone(report.spread_pair([smoke, repeat], "index", "warm", base))

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
        # The limit is 0.5: equal from 0.476 to 0.525.
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.51, 0.7, 0.05, 0.05, GATES), "equal")
        self.assertEqual(report.g7_verdict("selective", 100_000, 0.97, 0.7, 0.05, 0.05, GATES), "fail")

    def test_the_band_scales_the_limit_both_ways(self):
        # A spread of 1.2: one run's p50 was 2.2 × the other's. The band is 0.45 to 2.2, so a
        # ratio of 0.1 is still faster than the noise explains.
        self.assertEqual(report.g7_verdict("selective", 500, 0.1, 0.1, 1.2, 1.2, GATES), "pass")
        # The equal class uses the same band: 1.05 is on its upper edge.
        self.assertEqual(report.g7_verdict("equal", 500, 1.05, 1.0, 0.05, 0.05, GATES), "pass")

    def test_dense_and_equal_rules(self):
        # The dense limit is 1.10: equal from 1.048 to 1.155.
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


class MatchedRunsTest(unittest.TestCase):
    def test_compares_the_latest_index_run_with_runs_of_the_same_queries_and_repetitions(self):
        matrix = (found("1"), found("2", mode="bloom"), found("3", mode="today"))
        smoke = found("4", mode="bloom", used=20, reps=1)
        self.assertEqual(report.matched_runs([*matrix, smoke], "g7"), matrix)
        self.assertEqual(report.matched_runs([found("1"), smoke], "g7"), (found("1"), None, None))
        self.assertEqual(report.matched_runs([smoke], "g7"), (None, None, None))

    def test_pairs_runs_of_the_same_node_flags_only(self):
        # A cold index run on a low persistence threshold must not meet a bloom run on the default.
        low = ("node", "--engine.persistence-threshold", "0")
        index, bloom = found("1", flags=low), found("2", mode="bloom", flags=low)
        default = found("3", mode="bloom")
        self.assertEqual(report.matched_runs([index, bloom, default], "g7")[:2], (index, bloom))
        self.assertEqual(report.matched_runs([index, default], "g7")[:2], (index, None))

    def test_pairs_g1_runs_of_the_same_query_file(self):
        # G1 joins on query ids: a 1,000-query bloom run must not meet a 10,000-query index run.
        index, bloom = found("1", used=10_000, reps=1, kind="g1"), found("2", mode="bloom", used=10_000, reps=1, kind="g1")
        newer = found("3", mode="bloom", used=1_000, reps=1, kind="g1")
        self.assertEqual(report.matched_runs([index, bloom, newer], "g1")[:2], (index, bloom))


class LoadRunsTest(unittest.TestCase):
    def test_derives_the_node_flags_from_the_command_line(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "run-1"))
            with open(os.path.join(root, "run-1", "meta.json"), "w") as file:
                json.dump({"cmdline": ["/data/bin/reth", "node", "--filter-maps", "--chain", "mainnet"]}, file)
            self.assertEqual(report.load_runs(root)[0]["meta"]["node_flags"], ["node", "--chain", "mainnet"])


class G8RunsTest(unittest.TestCase):
    def test_pairs_the_loads_with_the_node_flags_of_the_index_load(self):
        index = found("1", command="load")
        bloom = found("2", mode="bloom", command="load")
        other = found("3", mode="bloom", command="load", flags=("node", "--engine.persistence-threshold", "0"))
        self.assertEqual(report.g8_runs([index, bloom, other]), {"index": index, "bloom": bloom, "today": None})


class G4RunsTest(unittest.TestCase):
    def test_takes_each_reference_with_the_node_flags_of_its_measured_run(self):
        catchup = found("1", command="watch", phase="catchup")
        off = found("2", mode="bloom", command="watch", phase="off")
        off_other = found("3", mode="bloom", command="watch", phase="off", flags=("node", "--engine.persistence-threshold", "0"))
        load_index = found("4", command="watch", phase="load")
        load_bloom = found("5", mode="bloom", command="watch", phase="load")
        self.assertEqual(
            report.g4_runs([catchup, off, off_other, load_index, load_bloom]),
            {("catchup", "index"): catchup, ("off", "bloom"): off, ("load", "index"): load_index, ("load", "bloom"): load_bloom},
        )
