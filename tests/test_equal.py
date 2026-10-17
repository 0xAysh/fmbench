import json
import os
import tempfile
import unittest

from fmbench import equal


def line(query_id, rep=0, digest="a", logs=1, error=None):
    return {"query_id": query_id, "rep": rep, "digest": None if error else digest, "logs": None if error else logs, "error": error}


class CompareTest(unittest.TestCase):
    def write(self, lines):
        file = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False)
        file.write("".join(json.dumps(entry) + "\n" for entry in lines))
        file.close()
        self.addCleanup(os.unlink, file.name)
        return file.name

    def test_lists_a_planted_difference_and_joins_on_the_first_repetition(self):
        a = self.write([line("Q1-w500-000"), line("Q3-w500-000", digest="b", logs=7), line("Q3-w500-000", rep=1, digest="x")])
        b = self.write([line("Q1-w500-000"), line("Q3-w500-000", digest="c", logs=7)])
        compared, differences = equal.compare(a, b)
        self.assertEqual(compared, 2)
        self.assertEqual(differences, ["Q3-w500-000: digest b != c"])

    def test_an_error_in_either_run_fails_the_comparison(self):
        a = self.write([line("Q1-w500-000"), line("Q9-w500-000")])
        b = self.write([line("Q1-w500-000"), line("Q9-w500-000", error="timeout")])
        self.assertEqual(equal.compare(a, b), (2, ["Q9-w500-000: error in b: timeout"]))

    def test_a_query_missing_from_one_run_is_a_difference(self):
        a = self.write([line("Q1-w500-000"), line("Q2-w500-000")])
        b = self.write([line("Q1-w500-000")])
        self.assertEqual(equal.compare(a, b), (2, ["Q2-w500-000: missing in b"]))
