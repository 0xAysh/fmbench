import json
import random
import unittest

from fmbench import sample


class WidthsTest(unittest.TestCase):
    def test_parses_block_counts_and_the_whole_window(self):
        self.assertEqual(sample.parse_widths("500,2000,10000,100000"), [500, 2000, 10000, 100000])
        self.assertEqual(sample.parse_widths("2000,window"), [2000, "window"])

    def test_rejects_anything_else(self):
        for text in ["", "0", "-5", "1e3", "500,", "wide"]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                sample.parse_widths(text)


TRANSFER = sample.TRANSFER
APPROVAL = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"


def addr(n):
    return "0x" + f"{n:040x}"


def word(n):
    """A 32-byte topic holding address `n`, as an indexed `address` parameter."""
    return "0x" + f"{n:064x}"


def topic(n):
    return "0x" + f"{n:x}".rjust(64, "f")


def log(address, *topics):
    return {"address": address, "topics": list(topics)}


def block(*logs):
    """One block's `eth_getBlockReceipts` result, one receipt per log."""
    return [{"logs": [entry]} for entry in logs]


class TallyTest(unittest.TestCase):
    def test_counts_addresses_topics_pairs_transfer_parties_and_emitters(self):
        upper = "0x" + "AB" * 20
        tally = sample.tally(
            [
                block(log(addr(1), TRANSFER, word(7), word(8)), log(upper, topic(1))),
                block(log(addr(1), TRANSFER, word(7), word(9)), log(addr(2))),
                block(),
            ]
        )
        self.assertEqual((tally.blocks, tally.logs), (3, 4))
        self.assertEqual(tally.address, {addr(1): 2, "0x" + "ab" * 20: 1, addr(2): 1})
        self.assertEqual(tally.topic0, {TRANSFER: 2, topic(1): 1})
        self.assertEqual(tally.pair, {(addr(1), TRANSFER): 2, ("0x" + "ab" * 20, topic(1)): 1})
        self.assertEqual(tally.sender, {word(7): 2})
        self.assertEqual(tally.recipient, {word(8): 1, word(9): 1})
        self.assertEqual(tally.emitters, {TRANSFER: {addr(1)}, topic(1): {"0x" + "ab" * 20}})


def window_logs(counts):
    """A whole-window `eth_getLogs` count lookup, keyed by the filter's JSON."""
    return lambda filter_: counts.get(json.dumps(filter_, sort_keys=True), 0)


def key(filter_):
    return json.dumps(filter_, sort_keys=True)


class RareTest(unittest.TestCase):
    def test_q1_takes_addresses_seen_once_with_1_to_100_logs_in_the_window_and_q2_adds_their_topic0(self):
        logs = [log(addr(n), topic(n)) for n in range(1, 9)] + [log(addr(20), topic(20))] * 2
        logs.append(log(addr(30)))  # seen once, but with no topic0 for Q2
        counts = {1: 1, 2: 100, 3: 101, 4: 40, 5: 7, 6: 0, 7: 3, 8: 5000}
        lookup = {key({"address": [addr(n)], "topics": []}): c for n, c in counts.items()}
        selection = sample.pick_filters(sample.tally([block(*logs)]), window_logs(lookup), random.Random(7))

        q1 = selection.filters["Q1"]
        self.assertEqual(
            sorted(pick.filter["address"][0] for pick in q1), [addr(1), addr(2), addr(4), addr(5), addr(7)]
        )
        self.assertEqual([pick.window_logs for pick in q1], [counts[int(p.filter["address"][0], 16)] for p in q1])
        self.assertEqual(
            [pick.filter for pick in selection.filters["Q2"]],
            [{"address": p.filter["address"], "topics": [topic(int(p.filter["address"][0], 16))]} for p in q1],
        )
        self.assertEqual({pick.rate for pick in q1 + selection.filters["Q2"]}, {1.0})


class PopularTest(unittest.TestCase):
    def setUp(self):
        # Address n emits 13 - n logs of topic n, n in 1..12; address 1 also emits 20 Approvals.
        logs = [log(addr(n), topic(n)) for n in range(1, 13) for _ in range(13 - n)]
        logs += [log(addr(1), APPROVAL)] * 20
        self.selection = sample.pick_filters(
            sample.tally([block(*logs)] * 2), window_logs({}), random.Random(7)
        )

    def test_q3_draws_five_of_the_top_ten_emitters_in_rank_order(self):
        q3 = self.selection.filters["Q3"]
        ranks = [int(pick.filter["address"][0], 16) for pick in q3]
        self.assertEqual(len(ranks), 5)
        self.assertEqual(ranks, sorted(ranks))
        self.assertLessEqual(max(ranks), 10)
        self.assertEqual([pick.filter["topics"] for pick in q3], [[]] * 5)
        self.assertEqual([pick.rate for pick in q3], [(13 - n + 20 * (n == 1)) for n in ranks])

    def test_q4_is_the_top_topic0_and_records_whether_it_is_transfer(self):
        self.assertEqual(self.selection.filters["Q4"], [sample.Pick({"topics": [APPROVAL]}, 20.0)])
        self.assertEqual(self.selection.notes["q4_topic0"], APPROVAL)
        self.assertFalse(self.selection.notes["q4_is_transfer"])

    def test_q5_draws_five_of_the_top_ten_address_topic0_pairs(self):
        q5 = [pick.filter for pick in self.selection.filters["Q5"]]
        top10 = [{"address": [addr(1)], "topics": [APPROVAL]}] + [
            {"address": [addr(n)], "topics": [topic(n)]} for n in range(1, 10)
        ]
        self.assertEqual(len(q5), 5)
        self.assertEqual(q5, [pair for pair in top10 if pair in q5])


class FactoryTest(unittest.TestCase):
    def test_q8_takes_the_non_token_topic0_with_most_emitters_at_50_200_and_1000_addresses(self):
        swap, other = topic(1), topic(2)
        logs = [log(addr(n), TRANSFER) for n in range(1, 400)]
        logs += [log(addr(n), APPROVAL) for n in range(1, 300)]
        # 120 swap emitters, address n with n % 7 + 1 logs; 100 emitters of another topic.
        logs += [log(addr(1000 + n), swap) for n in range(120) for _ in range(n % 7 + 1)]
        logs += [log(addr(2000 + n), other) for n in range(100)]
        selection = sample.pick_filters(sample.tally([block(*logs)]), window_logs({}), random.Random(7))

        ranked = sorted(range(120), key=lambda n: (-(n % 7 + 1), n))
        expected = [addr(1000 + n) for n in ranked]
        q8 = selection.filters["Q8"]
        self.assertEqual([pick.filter for pick in q8], [
            {"address": expected[:50], "topics": [swap]},
            {"address": expected, "topics": [swap]},
        ])
        self.assertEqual([pick.rate for pick in q8], [
            sum(n % 7 + 1 for n in ranked[:50]),
            sum(n % 7 + 1 for n in range(120)),
        ])
        self.assertEqual(selection.notes["q8_topic0"], swap)
        self.assertEqual(selection.notes["q8_addresses"], [50, 120])


class WalletTest(unittest.TestCase):
    def test_q9_takes_recipients_seen_once_with_1_to_100_logs_then_senders(self):
        token = addr(1)
        logs = [log(token, TRANSFER, word(60), r) for r in (word(1), word(2), word(3), word(4), topic(9))]
        logs += [log(token, TRANSFER, word(60), word(5))] * 2
        logs += [log(token, TRANSFER, word(50), word(70)), log(token, TRANSFER, word(51), word(70))]
        lookup = {
            key({"topics": [TRANSFER, None, word(1)]}): 3,
            key({"topics": [TRANSFER, None, word(2)]}): 500,
            key({"topics": [TRANSFER, None, word(3)]}): 40,
            key({"topics": [TRANSFER, None, topic(9)]}): 5,
            key({"topics": [TRANSFER, word(50)]}): 7,
            key({"topics": [TRANSFER, word(51)]}): 1000,
        }
        selection = sample.pick_filters(sample.tally([block(*logs)]), window_logs(lookup), random.Random(7))

        q9 = selection.filters["Q9"]
        self.assertEqual(
            sorted(key(pick.filter) for pick in q9),
            sorted([
                key({"topics": [TRANSFER, None, word(1)]}),
                key({"topics": [TRANSFER, None, word(3)]}),
                key({"topics": [TRANSFER, word(50)]}),
            ]),
        )
        self.assertEqual(q9[-1].filter, {"topics": [TRANSFER, word(50)]})
        self.assertEqual({pick.rate for pick in q9}, {1.0})


class HeadAndWildcardTest(unittest.TestCase):
    def test_q6_alternates_q1_and_q3_filters_and_q7_is_the_wildcard(self):
        logs = [log(addr(n), topic(n)) for n in range(1, 7)]
        logs += [log(addr(100 + n), topic(100)) for n in range(10) for _ in range(5)]
        lookup = {key({"address": [addr(n)], "topics": []}): 1 for n in range(1, 7)}
        selection = sample.pick_filters(sample.tally([block(*logs)]), window_logs(lookup), random.Random(7))

        q1, q3 = selection.filters["Q1"], selection.filters["Q3"]
        self.assertEqual(selection.filters["Q6"], [q1[0], q3[0], q1[1], q3[1], q1[2]])
        self.assertEqual(selection.filters["Q7"], [sample.Pick({"topics": []}, 56.0)])


WINDOW = (23_800_000, 26_150_123)


class G7Test(unittest.TestCase):
    def selection(self):
        rare = sample.Pick({"address": [addr(1)], "topics": []}, 0.0005, 41)
        dense = sample.Pick({"address": [addr(2)], "topics": []}, 30.0)
        return sample.Selection({"Q1": [rare], "Q3": [dense], "Q6": [rare, dense]}, {})

    def test_every_query_ends_at_to_block_and_the_cap_drops_dense_widths(self):
        kept, dropped = sample.templates(self.selection(), [500, 100_000, "window"], WINDOW, max_logs=200_000)
        self.assertEqual([t.query_id for t in dropped], ["Q3-w100000-000", "Q3-wwindow-000"])
        queries = sample.g7_queries(kept, WINDOW)
        self.assertEqual(
            [(q["id"], q["expected_logs"], q["filter"]["fromBlock"]) for q in queries],
            [
                ("Q1-w500-000", 0, hex(26_149_624)),
                ("Q1-w100000-000", 50, hex(26_050_124)),
                ("Q1-wwindow-000", 1175, hex(23_800_000)),
                ("Q3-w500-000", 15_000, hex(26_149_624)),
                ("Q6-w1-000", 0, hex(26_150_123)),
                ("Q6-w1-001", 30, hex(26_150_123)),
                ("Q6-w10-000", 0, hex(26_150_114)),
                ("Q6-w10-001", 300, hex(26_150_114)),
                ("Q6-w100-000", 0, hex(26_150_024)),
                ("Q6-w100-001", 3000, hex(26_150_024)),
            ],
        )
        self.assertEqual({q["filter"]["toBlock"] for q in queries}, {hex(26_150_123)})
        self.assertEqual(
            queries[1],
            {
                "id": "Q1-w100000-000",
                "class": "Q1",
                "width": 100_000,
                "expected_logs": 50,
                "filter": {
                    "fromBlock": hex(26_050_124),
                    "toBlock": hex(26_150_123),
                    "address": [addr(1)],
                    "topics": [],
                },
            },
        )

    def test_the_wildcard_runs_at_the_narrow_widths(self):
        selection = sample.Selection({"Q7": [sample.Pick({"topics": []}, 900.0)]}, {})
        queries = sample.g7_queries(sample.templates(selection, [500, 100_000], WINDOW, max_logs=200_000)[0], WINDOW)
        self.assertEqual(
            [(q["id"], q["expected_logs"]) for q in queries],
            [("Q7-w1-000", 900), ("Q7-w10-000", 9000), ("Q7-w100-000", 90_000)],
        )

    def test_a_width_wider_than_the_window_is_an_error(self):
        with self.assertRaises(ValueError):
            sample.templates(self.selection(), [3_000_000], WINDOW, max_logs=200_000)


class OneshotTest(unittest.TestCase):
    def test_holds_the_first_q1_and_q9_filters_over_the_whole_window(self):
        q1 = [sample.Pick({"address": [addr(n)], "topics": []}, 1 / 2000, n) for n in (4, 5)]
        q9 = [sample.Pick({"topics": [TRANSFER, None, word(n)]}, 1 / 2000, n) for n in (6, 7)]
        selection = sample.Selection({"Q1": q1, "Q3": [], "Q9": q9}, {})
        self.assertEqual(
            sample.oneshot_queries(selection, WINDOW),
            [
                {
                    "id": "Q1-wwindow-000",
                    "class": "Q1",
                    "width": "window",
                    "expected_logs": 4,
                    "filter": {
                        "fromBlock": hex(WINDOW[0]),
                        "toBlock": hex(WINDOW[1]),
                        "address": [addr(4)],
                        "topics": [],
                    },
                },
                {
                    "id": "Q9-wwindow-000",
                    "class": "Q9",
                    "width": "window",
                    "expected_logs": 6,
                    "filter": {
                        "fromBlock": hex(WINDOW[0]),
                        "toBlock": hex(WINDOW[1]),
                        "topics": [TRANSFER, None, word(6)],
                    },
                },
            ],
        )


class G1Test(unittest.TestCase):
    def test_draws_count_random_queries_inside_the_window_and_under_the_cap(self):
        rare = sample.Pick({"address": [addr(1)], "topics": []}, 0.0005, 41)
        dense = sample.Pick({"address": [addr(2)], "topics": []}, 30.0)
        selection = sample.Selection({"Q1": [rare], "Q3": [dense], "Q6": [rare]}, {})
        kept, _ = sample.templates(selection, [500, 100_000], WINDOW, max_logs=200_000)
        queries = sample.g1_queries(kept, WINDOW, 300, random.Random(7))

        self.assertEqual(len(queries), 300)
        self.assertEqual(len({q["id"] for q in queries}), 300)
        self.assertEqual({q["class"] for q in queries}, {"Q1", "Q3", "Q6"})
        self.assertEqual(
            {(q["class"], q["width"]) for q in queries},
            {("Q1", 500), ("Q1", 100_000), ("Q3", 500), ("Q6", 1), ("Q6", 10), ("Q6", 100)},
        )
        for q in queries:
            first, last = int(q["filter"]["fromBlock"], 16), int(q["filter"]["toBlock"], 16)
            self.assertEqual(last - first + 1, q["width"])
            self.assertGreaterEqual(first, WINDOW[0])
            self.assertLessEqual(last, WINDOW[1])
            self.assertEqual(sample.query_cell(q["id"]), (q["class"], q["width"]))
        self.assertGreater(len({q["filter"]["fromBlock"] for q in queries}), 250)


class QueryCellTest(unittest.TestCase):
    def test_reads_the_class_and_width_from_a_query_id(self):
        self.assertEqual(sample.query_cell("Q1-w100000-003"), ("Q1", 100_000))
        self.assertEqual(sample.query_cell("Q9-wwindow-000"), ("Q9", "window"))
        self.assertEqual(sample.query_cell("Q6-w10-001-00042"), ("Q6", 10))


class FakeNode:
    """Answers `eth_getBlockReceipts` with logs that depend only on the block number, and
    `eth_getLogs` with one log per 10,000 blocks of the range for any filter."""

    def call(self, method, params):
        if method == "eth_getBlockReceipts":
            n = int(params[0], 16)
            logs = [log(addr(n % 50 + k), topic(n % 7), word(n % 13), word(n)) for k in range(n % 4)]
            logs.append(log(addr(n), TRANSFER, word(n % 3), word(n + 1)))
            return block(*logs)
        if method == "eth_getLogs":
            blocks = int(params[0]["toBlock"], 16) - int(params[0]["fromBlock"], 16) + 1
            return [{}] * (blocks // 10_000)
        raise AssertionError(method)


class SampleFilesTest(unittest.TestCase):
    def files(self, seed):
        built = sample.build(
            FakeNode(),
            window=(1_000_000, 1_400_000),
            seed=seed,
            sampled_blocks=200,
            widths=[500, 2000, 10000, 100000],
            count=50,
            max_logs=200_000,
            node_commit="6ddddd1072",
        )
        return {kind: sample.format_queries(*built[kind]) for kind in ("g7", "g1", "oneshot")}

    def test_the_same_seed_gives_the_same_files(self):
        first = self.files(7)
        self.assertEqual(first, self.files(7))
        self.assertNotEqual(first["g1"], self.files(8)["g1"])

    def test_the_header_lists_the_queries_the_cap_dropped(self):
        built = sample.build(FakeNode(), (1_000_000, 1_400_000), 7, 200, [500, 2000, 10000, 100000], 50, 1_000, "6ddddd1072")
        header, queries = built["g7"]
        # One Transfer per block: Q4 has about 500 logs at 500 blocks and 2,000 at 2,000. The
        # popular emitters and pairs (Q3, Q5) pass the cap at 100,000 blocks, the factory topic
        # (Q8) at 10,000.
        self.assertEqual(
            header["dropped_by_cap"],
            [*(f"Q3-w100000-{i:03d}" for i in range(5)), "Q4-w2000-000", "Q4-w10000-000", "Q4-w100000-000",
             *(f"Q5-w100000-{i:03d}" for i in range(5)), "Q8-w10000-000", "Q8-w100000-000"],
        )
        kept = {q["id"] for q in queries}
        self.assertIn("Q4-w500-000", kept)
        self.assertFalse(kept & set(header["dropped_by_cap"]))

    def test_the_header_records_the_sample(self):
        header = json.loads(self.files(7)["g7"].splitlines()[0])["header"]
        self.assertEqual(
            {k: header[k] for k in ("kind", "window", "to_block", "seed", "sampled_blocks", "widths", "node_commit")},
            {
                "kind": "g7",
                "window": [1_000_000, 1_400_000],
                "to_block": 1_400_000,
                "seed": 7,
                "sampled_blocks": 200,
                "widths": [500, 2000, 10000, 100000],
                "node_commit": "6ddddd1072",
            },
        )
