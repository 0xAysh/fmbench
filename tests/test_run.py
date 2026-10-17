import hashlib
import json
import unittest

from fmbench import rpc, run

LOG_A = {
    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "topics": ["0xDDF252AD1BE2C89B69C2B068FC378DAA952BA7F163C4A11628F55A4DF523B3EF"],
    "data": "0x00FF",
    "blockNumber": "0x10",
    "blockHash": "0xAB",
    "blockTimestamp": "0x6543",
    "transactionHash": "0xCD",
    "transactionIndex": "0x2",
    "logIndex": "0x5",
    "removed": False,
}
LOG_B = dict(LOG_A, blockNumber="0x9", logIndex="0x7")


class DigestTest(unittest.TestCase):
    def test_hashes_the_kept_fields_lowercased_in_compact_json(self):
        canonical = (
            '[{"address":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","blockHash":"0xab",'
            '"blockNumber":"0x10","data":"0x00ff","logIndex":"0x5","removed":false,'
            '"topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],'
            '"transactionHash":"0xcd","transactionIndex":"0x2"}]'
        )
        self.assertEqual(run.digest([LOG_A]), hashlib.sha256(canonical.encode()).hexdigest())

    def test_does_not_depend_on_the_order_the_node_returns(self):
        self.assertEqual(run.digest([LOG_A, LOG_B]), run.digest([LOG_B, LOG_A]))

    def test_changes_with_every_kept_field_and_ignores_the_others(self):
        base = run.digest([LOG_A])
        for field, value in [
            ("address", "0x" + "11" * 20),
            ("topics", []),
            ("data", "0x"),
            ("blockNumber", "0x11"),
            ("blockHash", "0xac"),
            ("transactionHash", "0xce"),
            ("transactionIndex", "0x3"),
            ("logIndex", "0x6"),
            ("removed", True),
        ]:
            with self.subTest(field=field):
                self.assertNotEqual(run.digest([dict(LOG_A, **{field: value})]), base)
        self.assertEqual(run.digest([dict(LOG_A, blockTimestamp="0x1")]), base)


def scrape(text):
    from fmbench import prom

    return prom.parse("# TYPE reth_rpc_eth_filter_headers_read_total counter\n"
                      "# TYPE reth_rpc_eth_filter_fallbacks_total counter\n"
                      "# TYPE reth_filter_maps_maps_written counter\n" + text)


class CounterDeltaTest(unittest.TestCase):
    def test_keys_the_rpc_filter_deltas_by_name_and_path(self):
        before = scrape(
            'reth_rpc_eth_filter_headers_read_total{path="index"} 12\n'
            'reth_rpc_eth_filter_headers_read_total{path="bloom"} 100\n'
            "reth_rpc_eth_filter_fallbacks_total 0\n"
            "reth_filter_maps_maps_written 3\n"
        )
        after = scrape(
            'reth_rpc_eth_filter_headers_read_total{path="index"} 19\n'
            'reth_rpc_eth_filter_headers_read_total{path="bloom"} 100\n'
            "reth_rpc_eth_filter_fallbacks_total 1\n"
            "reth_filter_maps_maps_written 4\n"
        )
        self.assertEqual(
            run.counter_delta(before, after),
            {
                "reth_rpc_eth_filter_headers_read_total": {"index": 7, "bloom": 0},
                "reth_rpc_eth_filter_fallbacks_total": {"": 1},
            },
        )


class ResultLineTest(unittest.TestCase):
    def test_a_result_is_counted_and_digested(self):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "result": [LOG_A, LOG_B]}).encode()
        line = run.result_line("Q1-w500-000", 2, rpc.Response(200, body, 12.5, None), {})
        self.assertEqual(
            line,
            {
                "query_id": "Q1-w500-000",
                "rep": 2,
                "latency_ms": 12.5,
                "bytes": len(body),
                "logs": 2,
                "digest": run.digest([LOG_A, LOG_B]),
                "error": None,
                "counters": {},
            },
        )

    def test_a_timeout_has_no_digest(self):
        line = run.result_line("Q1-w500-000", 0, rpc.Response(None, b"", 1800000.0, "timeout"), {})
        self.assertEqual((line["error"], line["logs"], line["digest"]), ("timeout", None, None))

    def test_a_json_rpc_error_is_stored_with_no_digest(self):
        error = {"code": -32005, "message": "query exceeds max results 20000"}
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "error": error}).encode()
        line = run.result_line("Q3-w100000-000", 0, rpc.Response(200, body, 80.0, None), {})
        self.assertEqual((line["error"], line["logs"], line["digest"]), (error, None, None))
