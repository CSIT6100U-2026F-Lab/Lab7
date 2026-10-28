#!/usr/bin/env python3
"""Top-k tests. Run from Lab7: python3 test_topk.py

Each case runs Spark compute_topk(data/, k) and compares it to a
single-process full count. The starter local top-k map fails these.
"""

import os
import sys
import unittest
from collections import Counter

_BACKEND = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend")
sys.path.insert(0, _BACKEND)

from map_reduce import compute_topk, tokenize  # noqa: E402


def workspace_paths():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    return [
        os.path.join(data_dir, name)
        for name in sorted(os.listdir(data_dir))
        if os.path.isfile(os.path.join(data_dir, name)) and not name.startswith(".")
    ]


def gold_topk(paths, k):
    counts = Counter()
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                counts.update(tokenize(line))
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:k]


class TestTopK(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.paths = workspace_paths()

    def _assert_matches_gold(self, k):
        self.assertEqual(compute_topk(self.paths, k), gold_topk(self.paths, k))

    def test_k01(self):
        self._assert_matches_gold(1)

    def test_k02(self):
        self._assert_matches_gold(2)

    def test_k03(self):
        self._assert_matches_gold(3)

    def test_k04(self):
        self._assert_matches_gold(4)

    def test_k06(self):
        self._assert_matches_gold(6)

    def test_k08(self):
        self._assert_matches_gold(8)

    def test_k10(self):
        self._assert_matches_gold(10)

    def test_k12(self):
        self._assert_matches_gold(12)


if __name__ == "__main__":
    unittest.main(verbosity=2)
