from Utils import *
from scipy import stats
from scipy.stats import powerlaw, norm
import numpy as np


class CalculateScoreFactory(object):
    @staticmethod
    def get_calculate_score(insightType):
        if insightType == InsightType.POINT:
            return PointCalculateScore()
        elif insightType == InsightType.SHAPE:
            return ShapeCalcuateScore()
        else:
            return None


class CalculateScore(object):
    def sig(self, phi):
        raise Exception('Need to implement')

# TODO: check point sig correctness
class PointCalculateScore(CalculateScore):
    def sig(self, phi):
        X = [val for val in phi.value()]
        X -= max(X)
        power_law = power_law(X)
        gauss_distribute = norm.rvs(size = len(power_law))
        sig_t = 1 - stats.ttest_ind(power_law, gauss_distribute)[1]
        return sig_t

# TODO: shape sig
class ShapeCalcuateScore(CalculateScore):
    def sig(self, phi):
        pass
