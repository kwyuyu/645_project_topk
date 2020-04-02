import collections
from Utils import *


class AggregateFunctionFactory(object):
    @staticmethod
    def get_aggregate_function(aggregate_type):
        if aggregate_type == AggregateType.SUM:
            return Sum()
        elif aggregate_type == AggregateType.RANK:
            return Rank()
        elif aggregate_type == AggregateType.PERCENTILE:
            return Percentile()
        elif aggregate_type == AggregateType.DELTA_AVG:
            return DeltaAvg()
        elif aggregate_type == AggregateType.DELTA_PREV:
            return DeltaPrev()
        else:
            return None


class AggregateFunction(object):
    def measurement(self, phi):
        """

        :param phi:
        :type phi: dictionary => Subspace: value
        """
        raise Exception('Need to implement')


class Sum(AggregateFunction):
    def measurement(self, phi):
        raise Exception('Need to implement')


class Rank(AggregateFunction):
    def measurement(self, phi):
        output_phi = collections.OrderedDict()
        sort_phi_value = sorted(phi, key = phi.__getitem__, reverse = True)
        for i, S in enumerate(sort_phi_value):
            output_phi[S] = i + 1
        return output_phi


class Percentile(AggregateFunction):
    def measurement(self, phi):
        output_phi = collections.OrderedDict()
        total = sum([val for val in phi.value()])
        for S, val in phi.items():
            output_phi[S] = (val / total) * 100
        return output_phi


class DeltaAvg(AggregateFunction):
    def measurement(self, phi):
        output_phi = collections.OrderedDict()
        avg = sum([val for val in phi.value()]) / len(phi)
        for S, val in phi.items():
            output_phi[S] = val - avg
        return output_phi


class DeltaPrev(AggregateFunction):
    def measurement(self, phi):
        output_phi = collections.OrderedDict()
        for i, S in enumerate(list(phi)):
            if i == 0:
                output_phi[S] = 0
            else:
                output_phi[S] = phi[i] - phi[i-1]
        return output_phi

