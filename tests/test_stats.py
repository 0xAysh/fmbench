import unittest

from fmbench import stats


class PercentileTest(unittest.TestCase):
    def test_interpolates_between_the_closest_ranks(self):
        values = [7, 1, 3, 2, 10, 4, 6, 5, 9, 8]
        self.assertEqual(stats.percentile(values, 0.5), 5.5)
        self.assertAlmostEqual(stats.percentile(values, 0.9), 9.1)
        self.assertAlmostEqual(stats.percentile(values, 0.99), 9.91)
        self.assertEqual(stats.percentile(values, 0), 1)
        self.assertEqual(stats.percentile(values, 1), 10)

    def test_one_value_and_no_values(self):
        self.assertEqual(stats.percentile([42.0], 0.99), 42.0)
        self.assertIsNone(stats.percentile([], 0.5))
