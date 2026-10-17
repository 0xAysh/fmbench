import random
import unittest
from collections import Counter

from fmbench import load


def query(cls, width, n=0, last=26_150_123):
    blocks = 2_350_124 if width == "window" else width
    return {
        "id": f"{cls}-w{width}-{n:03d}",
        "class": cls,
        "width": width,
        "filter": {"fromBlock": hex(last - blocks + 1), "toBlock": hex(last), "topics": []},
    }


QUERIES = [query(cls, w, n) for cls in ("Q1", "Q3", "Q4", "Q9") for w in (500, 100_000) for n in range(3)]


class MixTest(unittest.TestCase):
    def test_draws_classes_in_proportion_to_their_weights(self):
        mix = load.Mix(QUERIES, {"Q1": 20, "Q3": 10, "Q9": 20, "Q8": 15})
        rng = random.Random(3)
        drawn = Counter(mix.draw(rng)["class"] for _ in range(20_000))
        self.assertEqual(set(drawn), {"Q1", "Q3", "Q9"})
        for cls, share in {"Q1": 0.4, "Q3": 0.2, "Q9": 0.4}.items():
            self.assertAlmostEqual(drawn[cls] / 20_000, share, delta=0.015)
        self.assertEqual(mix.missing, ["Q8"])

    def test_the_same_seed_draws_the_same_queries(self):
        mix = load.Mix(QUERIES, load.HEAVY)

        def draws(seed):
            rng = random.Random(seed)
            return [mix.draw(rng)["id"] for _ in range(50)]

        self.assertEqual(draws(5), draws(5))
        self.assertNotEqual(draws(5), draws(6))

    def test_never_draws_a_query_wider_than_the_cap(self):
        queries = QUERIES + [query("Q1", "window"), query("Q9", 100_001)]
        mix = load.Mix(queries, load.HEAVY)
        rng = random.Random(1)
        widths = {mix.draw(rng)["width"] for _ in range(5_000)}
        self.assertEqual(widths, {500, 100_000})


def record(t_start, latency_ms, cls="Q1", status="ok", client=0):
    return {"client": client, "t_start": t_start, "latency_ms": latency_ms, "class": cls, "status": status}


class SummaryTest(unittest.TestCase):
    def test_qps_counts_answers_inside_the_window_and_latency_the_requests_started_in_it(self):
        records = [
            record(-0.5, 1000.0),  # warmup start, answered at 0.5 s: counts for QPS only
            *[record(i, 10.0 * (i + 1), "Q1" if i % 2 else "Q9") for i in range(10)],
            record(9.5, 2000.0, "Q5"),  # answered after the window: latency only
            record(3.0, 5.0, status="timeout"),
        ]
        summary = load.summarize(records, clients=1, duration_s=10, warmup_s=1, cpu=[0.3], missing=["Q8"])
        self.assertEqual(summary["requests"], 12)
        self.assertEqual(summary["answered_in_window"], 11)
        self.assertEqual(summary["qps"], 1.1)
        self.assertEqual(summary["statuses"], {"ok": 11, "timeout": 1})
        self.assertEqual(summary["latency_ms"]["Q1"], {"p50": 60.0, "p95": 96.0, "p99": 99.2})
        self.assertEqual(summary["latency_ms"]["Q9"]["p50"], 50.0)
        self.assertEqual(summary["latency_ms"]["all"]["p50"], 60.0)
        self.assertAlmostEqual(summary["latency_ms"]["all"]["p99"], 1810.0)
        self.assertEqual((summary["client_cpu_max"], summary["client_cpu_flag"]), (0.3, False))
        self.assertEqual(summary["missing_classes"], ["Q8"])

    def test_flags_a_client_above_80_percent_of_a_core(self):
        summary = load.summarize([record(0, 1.0)], clients=2, duration_s=10, warmup_s=1, cpu=[0.5, 0.85], missing=[])
        self.assertTrue(summary["client_cpu_flag"])
