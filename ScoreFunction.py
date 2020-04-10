import enum
import numpy as np
from scipy import stats
from scipy.stats import powerlaw, norm
from typing import *
from CustomType import *
from Utils import *



'''Enum'''
class InsightType(enum.Enum):
    POINT = 0
    SHAPE = 1


'''interface'''
class ScoreCalculator(object):
    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        """

        :param phi:
        :type phi: OrderedDict => Subspace: value
        :return:
        :rtype: float
        """
        raise NotImplemented('Need to implement')


# TODO: check point sig correctness
class PointScoreCalculator(ScoreCalculator):
    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        X = [val for val in phi.value()]
        X -= max(X)
        power_law = power_law(X)
        gauss_distribute = norm.rvs(size = len(power_law))
        sig_t = 1 - stats.ttest_ind(power_law, gauss_distribute)[1]
        return sig_t


# TODO: shape sig
class ShapeScoreCalculator(ScoreCalculator):
    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        pass


'''Factory'''
class ScoreCalculatorFactory(object):
    @staticmethod
    def get_score_calculator(insightType: InsightType) -> ScoreCalculator:
        """

        :param insightType:
        :type insightType: InsightType
        :return:
        :rtype: ScoreCalculator
        """
        if insightType == InsightType.POINT:
            return PointScoreCalculator()
        elif insightType == InsightType.SHAPE:
            return ShapeScoreCalculator()
        else:
            return None
