import unittest
import numpy as np
from mts.stats.similarity import jaccard_similarity, jaccard_distance, jaccard


class TestJaccard(unittest.TestCase):
    def test_array(self):
        a = np.array([1, 0, 0, 1, 1, 1])
        b = np.array([0, 0, 1, 1, 1, 0])
        similarity = jaccard_similarity(a, b)
        distance = jaccard_distance(a, b)
        self.assertEqual(jaccard(a, b), similarity)
        self.assertEqual(similarity, 0.4)
        self.assertEqual(distance, 0.6)

    def test_set(self):
        a = {1, 2, 3, 5, 7}
        b = {1, 2, 4, 8, 9}
        similarity = jaccard_similarity(a, b)
        distance = jaccard_distance(a, b)
        self.assertEqual(jaccard(a, b, True), distance)
        self.assertEqual(similarity, 0.25)
        self.assertEqual(distance, 0.75)

    def test_error(self):
        with self.assertRaises(TypeError):
            jaccard_distance(1, 2)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
