import enum
import numpy as np
from scipy import stats
from scipy.stats import powerlaw, norm



class InsightType(enum.Enum):
    POINT = 0
    SHAPE = 1


class ScoreCalculatorFactory(object):
    @staticmethod
    def get_score_calculator(insightType):
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


class ScoreCalculator(object):
    def sig(self, phi):
        """

        :param phi:
        :type phi: OrderedDict => Subspace: value
        """
        raise Exception('Need to implement')

# TODO: check point sig correctness
class PointScoreCalculator(ScoreCalculator):
    def sig(self, phi):
        X = [val for val in phi.value()]
        X -= max(X)
        power_law = power_law(X)
        gauss_distribute = norm.rvs(size = len(power_law))
        sig_t = 1 - stats.ttest_ind(power_law, gauss_distribute)[1]
        return sig_t

# TODO: shape sig
class ShapeScoreCalculator(ScoreCalculator):
    def sig(self, phi):
        pass
