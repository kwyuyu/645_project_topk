from __future__ import annotations

import collections
import enum
from typing import *
from abc import *


if TYPE_CHECKING:
    from CustomizedType import *


'''Enum'''
class AggregateType(enum.Enum):
    SUM = 0
    RANK = 1
    PERCENTILE = 2
    DELTA_AVG = 3
    DELTA_PREV = 4

    @staticmethod
    def get_aggregate_types() -> List[AggregateType]:
        output = []
        for i, aggr_type in enumerate(AggregateType):
            if aggr_type != AggregateType.SUM:
                output.append(aggr_type)
        return output


'''Interface'''
class AggregateFunction(ABC):
    @abstractmethod
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> OrderedDict[Subspace, Number]:
        """

        :param phi:
        :type phi: OrderedDict => Subspace: value
        :return:
        :rtype: float
        """
        raise NotImplemented('Need to implement')


class Rank(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> OrderedDict[Subspace, Number]:
        output_phi = collections.OrderedDict()
        sort_phi_value = sorted(phi, key = phi.__getitem__, reverse = True)
        for i, S in enumerate(sort_phi_value):
            output_phi[S] = i + 1
        return output_phi


class Percentile(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> OrderedDict[Subspace, Number]:
        output_phi = collections.OrderedDict()
        total = sum([val for val in phi.values()])
        for S, val in phi.items():
            output_phi[S] = (val / total) * 100
        return output_phi


class DeltaAvg(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> OrderedDict[Subspace, Number]:
        output_phi = collections.OrderedDict()
        avg = sum([val for val in phi.values()]) / len(phi)
        for S, val in phi.items():
            output_phi[S] = val - avg
        return output_phi


class DeltaPrev(AggregateFunction):
    def measurement(self, phi: OrderedDict[Subspace, Number]) -> OrderedDict[Subspace, Number]:
        output_phi = collections.OrderedDict()
        prev_S = None
        for i, S in enumerate(list(phi)):
            if i == 0:
                output_phi[S] = 0
            else:
                output_phi[S] = phi[S] - phi[prev_S]
            prev_S = S
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