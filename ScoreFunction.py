from __future__ import annotations

import random
import enum
import math
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


class PointScoreCalculator(ScoreCalculator):
    def powerLaw(self, x, shape, loc, scale):
        return scale * x**shape + loc

    def cubicEq(self, x, a, b, c, d):
        return a * x ** 3 + b * x ** 2 + c * x + d

    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        """
        Algorithm:
            1. Sort numbers
            2. Fit numbers to power-law distribution
            3. Model the prediction error by Gaussian distribution (N(mu, delta))
        
        Special Cases:
            1. Data points are not enough for prediction
            2. Prediction perfectly match y => mu ~ std ~ 0, the error distribution is a horizontal line
            3. Prediction perfectly match y[1:] but not y[0] => mu ~ 0 = std, no error distribution
        """
        phi_val = list(phi.values())
        # Case 1: too few data points
        if len(phi_val) < 4: return 0.0

        y = sorted(phi_val, reverse=True)
        x = list(range(1, len(y)+1))

        try:
            param_opt, pcov = curve_fit(self.powerLaw, x[1:], y[1:], maxfev=2000)
            func = self.powerLaw
        except:
            try:
                param_opt, pcov = curve_fit(self.cubicEq, x[1:], y[1:], maxfev=2000)
                func = self.cubicEq
            except:
                import ipdb;ipdb.set_trace()

        try:
            errors = y - func(x, *param_opt)
        except: 
            print(y)
            #import ipdb;ipdb.set_trace()
        # Case 2: If the prediction perfectly match, the error will be too small but not equals to zero.
        if errors[0] < math.exp(-9): return 0.0
        mu, std = norm.fit(errors[1:])
        # Case 3
        if std < math.exp(-9): return 1.0

        sig_score = norm(mu, std).cdf(errors[0])
        return sig_score


class ShapeScoreCalculator(ScoreCalculator):
    def __init__(self):
        self.lr = linear_model.LinearRegression()

    def sig(self, phi: OrderedDict[Subspace, Number]) -> Number:
        '''
        Algorithm:
            1. Fit numbers to a line by linear regression
            2. Compute slope m and the goodness-of-fit r^2
            3. Model the slopes by logistic distribution
        '''
        y = np.array(list(phi.values()))
        x = np.arange(1, len(y)+1).reshape(-1, 1)
        reg = self.lr.fit(x, y)
        r2 = reg.score(x, y)
        errors = y - reg.predict(x)
        mu, std = logistic.fit(errors[1:])
        sig_score = r2 * logistic(mu, std).cdf(errors[0])
        return sig_score


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
