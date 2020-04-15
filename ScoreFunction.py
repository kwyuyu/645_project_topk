from __future__ import annotations

import enum
import numpy as np
from scipy import stats
from scipy.stats import powerlaw, norm
from typing import *
from abc import *


if TYPE_CHECKING:
    from Utils import *
    from CustomizedType import *



'''Enum'''
class InsightType(enum.Enum):
    POINT = 0
    SHAPE = 1



'''interface'''
class ScoreCalculator(ABC):
    @abstractmethod
    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        """

        :param phi:
        :type phi: OrderedDict => Subspace: value
        :return:
        :rtype: float
        """
        raise NotImplemented('Need to implement')


# TODO: point sig
class PointScoreCalculator(ScoreCalculator):
    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        import random
        return random.random()
        raise NotImplemented('Need to implement')


# TODO: shape sig
class ShapeScoreCalculator(ScoreCalculator):
    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        import random
        return random.random()
        raise NotImplemented('Need to implement')


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
