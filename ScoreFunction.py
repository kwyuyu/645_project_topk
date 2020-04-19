from __future__ import annotations

import enum
import numpy as np
from scipy import stats
from scipy.stats import powerlaw
from scipy.stats import norm, logistic
from scipy.optimize import curve_fit
from sklearn import linear_model
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
    def powerlaw(self, x, shape, loc, scale):
        return scale * x**shape + loc

    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        """
        1. sort numbers
        2. fit numbers to power-law distribution
        3. model the prediction error by Gaussian distribution (N(mu, delta))
        """
        # import random
        # return random.random()

        phi_val = list(phi.values())
        y = sorted(phi_val, reverse=True)
        n = len(y)
        x = list(range(1, n+1))
        param_opt, pcov = curve_fit(self.powerlaw, x[1:], y[1:], maxfev=3000)
        errors = y - self.powerlaw(x, *param_opt)
        mu, std = norm.fit(errors[1:])
        return norm(mu, std).cdf(errors[0])

        # y = np.array(sorted(list(set(phi.values())), reverse=True)).reshape(-1, 1)
        # n = len(y)
        # x = np.array(list(range(1, n + 1))).reshape(-1, 1)
        # param_opt, pcov = curve_fit(self.powerlaw, x[1:], y[1:], maxfev=3000)
        # errors = y - self.powerlaw(x, *param_opt)
        # mu, std = norm.fit(errors[1:])
        # return norm(mu, std).cdf(errors[0])


# TODO: shape sig
class ShapeScoreCalculator(ScoreCalculator):
    def __init__(self):
        self.lr = linear_model.LinearRegression()

    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        '''
        1. fit numbers to a line by linear regression
        2. compute slope m and the goodness-of-fit r^2
        3. model the slopes by logistic distribution
        '''
        # import random
        # return random.random()

        # phi_val = list(phi.values())
        # y = phi_val[:]
        # n = len(y)
        # x = list(range(1, n+1))
        # reg = self.lr.fit(x, y)
        # r2 = reg.score(x, y)
        # errors = y - reg.predict(x)
        # mu, std = logistic.fit(errors[1:])
        # return r2 * logistic(mu, std).cdf(errors[0])

        phi_val = list(phi.values())
        y = np.array(phi_val).reshape(-1, 1)
        n = len(y)
        x = np.array(list(range(1, n + 1))).reshape(-1, 1)
        reg = self.lr.fit(x, y)
        r2 = reg.score(x, y)
        errors = y - reg.predict(x)
        mu, std = logistic.fit(errors[1:])
        score = r2 * logistic(mu, std).cdf(errors[0])
        return score[0]


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
