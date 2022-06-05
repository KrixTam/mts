import numpy as np
from scipy.spatial.distance import jaccard as jaccard_distance_np
from mts.commons import logger


def jaccard_similarity(a, b):
    return 1 - jaccard_distance(a, b)


def jaccard_distance(a, b):
    if isinstance(a, set) and isinstance(b, set):
        nominator = a.symmetric_difference(b)
        denominator = a.union(b)
        return len(nominator) / len(denominator)
    else:
        if isinstance(a, np.ndarray) and isinstance(b, np.ndarray):
            return jaccard_distance_np(a, b)
        else:
            raise TypeError(logger.warning([6100]))


def jaccard(a, b, distance: bool = False):
    if distance:
        return jaccard_distance(a, b)
    else:
        return jaccard_similarity(a, b)
