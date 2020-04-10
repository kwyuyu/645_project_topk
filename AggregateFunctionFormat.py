import collections
import enum
from Utils import *
from typing import *
from CustomType import *


'''Enum'''
class AggregateType(enum.Enum):
    RANK = 0
    PERCENTILE = 1
    DELTA_AVG = 2
    DELTA_PREV = 3


'''Interface'''
class AggregateFunction(object):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> Number:
        """

        :param phi:
        :type phi: OrderedDict => Subspace: value
        :return:
        :rtype: float
        """
        raise NotImplemented('Need to implement')


class Rank(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> Number:
        output_phi = collections.OrderedDict()
        sort_phi_value = sorted(phi, key = phi.__getitem__, reverse = True)
        for i, S in enumerate(sort_phi_value):
            output_phi[S] = i + 1
        return output_phi


class Percentile(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> Number:
        output_phi = collections.OrderedDict()
        total = sum([val for val in phi.value()])
        for S, val in phi.items():
            output_phi[S] = (val / total) * 100
        return output_phi


class DeltaAvg(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> float:
        output_phi = collections.OrderedDict()
        avg = sum([val for val in phi.value()]) / len(phi)
        for S, val in phi.items():
            output_phi[S] = val - avg
        return output_phi


class DeltaPrev(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> float:
        output_phi = collections.OrderedDict()
        for i, S in enumerate(list(phi)):
            if i == 0:
                output_phi[S] = 0
            else:
                output_phi[S] = phi[i] - phi[i-1]
        return output_phi


'''Factory'''
class AggregateFunctionFactory(object):
    @staticmethod
    def get_aggregate_function(aggregate_type: AggregateType) -> AggregateFunction:
        """

        :param aggregate_type:
        :type aggregate_type: AggregateType
        :return:
        :rtype: AggregateFunction
        """
        if aggregate_type == AggregateType.RANK:
            return Rank()
        elif aggregate_type == AggregateType.PERCENTILE:
            return Percentile()
        elif aggregate_type == AggregateType.DELTA_AVG:
            return DeltaAvg()
        elif aggregate_type == AggregateType.DELTA_PREV:
            return DeltaPrev()
        else:
            return None