import unittest

from fmbench import watch

# A line of `reth node --log.file.format json` (tracing-subscriber's JSON layer).
BLOCK_LINE = (
    '{"timestamp":"2026-10-17T04:14:03.512345Z","level":"INFO","fields":{"message":"Block added to '
    'canonical chain","number":26209812,"hash":"0x5c1f","peers":97,"txs":211,"gas_used":"29.94Mgas",'
    '"gas_throughput":"663.86Mgas/second","gas_limit":"45.00Mgas","full":"66.5%","base_fee":"0.29Gwei",'
    '"blobs":6,"excess_blobs":0,"elapsed":"45.101234ms"},"target":"reth_node_events::node"}'
)


class DurationTest(unittest.TestCase):
    def test_parses_every_unit_of_a_rust_duration_debug_string(self):
        self.assertEqual(watch.parse_duration_ns("45.101234ms"), 45_101_234)
        self.assertEqual(watch.parse_duration_ns("1.5s"), 1_500_000_000)
        self.assertEqual(watch.parse_duration_ns("850.25µs"), 850_250)
        self.assertEqual(watch.parse_duration_ns("850us"), 850_000)
        self.assertEqual(watch.parse_duration_ns("500ns"), 500)
        self.assertEqual(watch.parse_duration_ns("0ns"), 0)

    def test_rejects_anything_else(self):
        for text in ["45", "1.5 s", "2m", "ms"]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                watch.parse_duration_ns(text)


class BlockLineTest(unittest.TestCase):
    def test_reads_the_number_and_execution_time_of_a_canonical_block(self):
        self.assertEqual(watch.block_line(BLOCK_LINE), (26209812, 45_101_234))

    def test_skips_other_lines(self):
        self.assertIsNone(watch.block_line('{"fields":{"message":"Forkchoice updated"}}'))
        self.assertIsNone(watch.block_line("not json"))


FM = "reth_filter_maps_"


def sample(elapsed_s, rendered, maps, step_sum, step_count, np_sum, np_count, block_ns=()):
    return watch.Sample(
        elapsed_s=elapsed_s,
        series={
            FM + "first_indexed_block": 23_800_000,
            FM + "last_indexed_block": 23_900_000,
            FM + "store_target": 23_900_040,
            FM + "blocks_rendered": rendered,
            FM + "maps_written": maps,
            FM + "step_duration_seconds_sum": step_sum,
            FM + "step_duration_seconds_count": step_count,
        },
        finalized=23_900_064,
        store_bytes=1_000_010_000,
        block_ns=list(block_ns),
        new_payload=(np_sum, np_count),
    )


class DeriveTest(unittest.TestCase):
    def test_derives_the_interval_rates_the_lag_and_the_block_times(self):
        before = sample(20.0, 1_000, 10, 1.0, 50, 10.0, 100)
        after = sample(30.0, 4_000, 110, 6.0, 150, 10.4, 104, block_ns=[10e6, 20e6, 30e6, 40e6])
        derived = watch.derive(before, after)
        self.assertEqual(
            {k: derived[k] for k in ("blocks_per_s", "maps_per_s", "step_mean_s", "lag_blocks", "bytes_per_block", "block_count", "block_p50_ms")},
            {
                "blocks_per_s": 300.0,
                "maps_per_s": 10.0,
                "step_mean_s": 0.05,
                "lag_blocks": 40,
                "bytes_per_block": 10_000.0,
                "block_count": 4,
                "block_p50_ms": 25.0,
            },
        )
        # 4,000 blocks rendered into 110 maps since the node started: 64 blocks behind finalized.
        self.assertAlmostEqual(derived["lag_maps"], 64 / (4_000 / 110))
        self.assertAlmostEqual(derived["block_p99_ms"], 39.7)
        self.assertAlmostEqual(derived["new_payload_mean_ms"], 100.0)
        self.assertIsNone(derived["write_mean_s"])

    def test_the_first_sample_and_an_index_that_is_off_have_no_rates(self):
        off = watch.Sample(0.0, {}, 23_900_064, None, [5e6], (1.0, 10))
        derived = watch.derive(None, off)
        self.assertEqual(derived["block_p50_ms"], 5.0)
        self.assertEqual(
            {k for k, v in derived.items() if v is not None}, {"block_count", "block_p50_ms", "block_p99_ms"}
        )
