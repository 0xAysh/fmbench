import unittest

from fmbench import prom

METRICS = """\
# HELP reth_rpc_eth_filter_headers_read_total The number of headers read.
# TYPE reth_rpc_eth_filter_headers_read_total counter
reth_rpc_eth_filter_headers_read_total{path="index"} 12
reth_rpc_eth_filter_headers_read_total{path="bloom"} 100000
# TYPE reth_rpc_eth_filter_fallbacks_total counter
reth_rpc_eth_filter_fallbacks_total 0
# TYPE reth_filter_maps_last_indexed_block gauge
reth_filter_maps_last_indexed_block 26150123
# TYPE reth_rpc_eth_filter_match_duration_seconds summary
reth_rpc_eth_filter_match_duration_seconds{quantile="0"} 0.001
reth_rpc_eth_filter_match_duration_seconds{quantile="0.5"} 0.002
reth_rpc_eth_filter_match_duration_seconds{quantile="0.99"} 0.009
reth_rpc_eth_filter_match_duration_seconds_sum 0.25
reth_rpc_eth_filter_match_duration_seconds_count 100
"""


class ParseTest(unittest.TestCase):
    def test_keys_series_by_name_and_labels(self):
        snapshot = prom.parse(METRICS)
        self.assertEqual(
            snapshot[("reth_rpc_eth_filter_headers_read_total", frozenset({("path", "index")}))], 12
        )
        self.assertEqual(
            snapshot[("reth_rpc_eth_filter_headers_read_total", frozenset({("path", "bloom")}))],
            100000,
        )
        self.assertEqual(snapshot[("reth_filter_maps_last_indexed_block", frozenset())], 26150123)
        self.assertEqual(
            snapshot[
                ("reth_rpc_eth_filter_match_duration_seconds", frozenset({("quantile", "0.99")}))
            ],
            0.009,
        )
        self.assertEqual(snapshot[("reth_rpc_eth_filter_match_duration_seconds_count", frozenset())], 100)
        self.assertEqual(len(snapshot), 9)


class CountersTest(unittest.TestCase):
    def test_keeps_counters_and_summary_totals_with_the_prefix(self):
        counters = prom.counters(prom.parse(METRICS), "reth_rpc_eth_filter_")
        self.assertEqual(
            counters,
            {
                ("reth_rpc_eth_filter_headers_read_total", frozenset({("path", "index")})): 12,
                ("reth_rpc_eth_filter_headers_read_total", frozenset({("path", "bloom")})): 100000,
                ("reth_rpc_eth_filter_fallbacks_total", frozenset()): 0,
                ("reth_rpc_eth_filter_match_duration_seconds_sum", frozenset()): 0.25,
                ("reth_rpc_eth_filter_match_duration_seconds_count", frozenset()): 100,
            },
        )


class DeltaTest(unittest.TestCase):
    def test_subtracts_each_series_and_counts_a_new_series_from_zero(self):
        index = ("reth_rpc_eth_filter_headers_read_total", frozenset({("path", "index")}))
        bloom = ("reth_rpc_eth_filter_headers_read_total", frozenset({("path", "bloom")}))
        before = {index: 12, bloom: 100000}
        after = {index: 19, bloom: 100000, ("reth_rpc_eth_filter_fallbacks_total", frozenset()): 1}
        self.assertEqual(
            prom.delta(before, after),
            {index: 7, bloom: 0, ("reth_rpc_eth_filter_fallbacks_total", frozenset()): 1},
        )


class IndexedWindowTest(unittest.TestCase):
    def window(self, text):
        return prom.indexed_window(prom.parse(text))

    def test_reads_the_first_and_last_indexed_block_from_genesis_on(self):
        text = "reth_filter_maps_first_indexed_block 0\nreth_filter_maps_last_indexed_block 26150123\n"
        self.assertEqual(self.window(text), (0, 26150123))

    def test_no_index_when_the_index_is_off_or_holds_no_block(self):
        self.assertIsNone(self.window(""))
        self.assertIsNone(self.window("reth_filter_maps_first_indexed_block 0\nreth_filter_maps_last_indexed_block 0\n"))
