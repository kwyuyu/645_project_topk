from __future__ import annotations

import collections
from typing import *

from Heap import *
from Utils import *
from ScoreFunction import *
from AggregateFunctionFormat import *
from AttributeValueFormat import *
from DatabaseOperation import *


if TYPE_CHECKING:
    from CustomizedType import *


def timer(method_name):
    def decorator(func):
        import functools
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import time
            print('start: %s...' % (method_name))
            start = time.time()
            result = func(*args, **kwargs)
            print('complete: %s... %f sec\n' % (method_name, time.time() - start))
            return result
        return wrapper
    return decorator


class TopKInsight(object):
    def __init__(self, DB: Database):
        """

        :param DB:
        :type DB: Database
        """
        self.__adj_extractors = {AggregateType.RANK: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV],
                                 AggregateType.PERCENTILE: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV],
                                 AggregateType.DELTA_AVG: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV],
                                 AggregateType.DELTA_PREV: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV]}

        self.__DB = DB

        self.__dom = dict() # Di: list() of AttributeValue

        self.__depth = 0

        self.__subspace_dimension = 0
        self.__subspace_attr_ids = []
        self.__measurement_attr_id = -1

        self.__table_name = None
        self.__table_dimension = 0
        self.__table_column_names = []


    '''Main algorithm'''
    def insights(self, table_name: str, result_size: int, insight_dimension: List[int]) -> List[ComponentExtractor]:
        """

        :param table_name:
        :type table_name: str
        :param result_size:
        :type result_size: int
        :param insight_dimension: the dimension that each Extractor used to measure.
        :type insight_dimension: List[int], ex. [measurement, D0, D1, D2, ...]
        :return:
        :rtype: sorted List[ComponentExtractor]
        """
        self.__initialization(table_name, insight_dimension)

        heap = Heap(result_size)
        possible_Ce = self.__enumerate_all_Ce()

        for Ce in possible_Ce:
            for subspace_id in range(len(self.__subspace_attr_ids)):
                S = Subspace.create_all_start_subspace(self.__subspace_dimension)
                self.__enumerate_insight(S, subspace_id, Ce, heap)

        return heap.get_nlargest()


    @timer('initialization')
    def __initialization(self, table_name: str, insight_dimension: List[int]):
        """

        :param insight_dimension:
        :type insight_dimension: List[int], ex. [measurement, D0, D1, D2, ...]
        :param table_name:
        :type table_name: str
        """
        self.__depth = len(insight_dimension)
        self.__table_name = table_name
        self.__table_column_names = self.__get_table_column_names()
        self.__table_dimension = len(self.__table_column_names)
        self.__subspace_dimension = len(insight_dimension) - 1
        self.__measurement_attr_id = insight_dimension[0]

        for subspace_attr_id in insight_dimension[1:]:
            self.__subspace_attr_ids.append(subspace_attr_id)
            if subspace_attr_id not in self.__dom:
                self.__dom[subspace_attr_id] = list()
                raw_output = self.__DB.execute('select distinct %s from %s;' % (self.__table_column_names[subspace_attr_id], self.__table_name))
                for attr_val in list(map(lambda x: x[0], raw_output)):
                    self.__dom[subspace_attr_id].append(AttributeValueFactory.get_attribute_value(attr_val))


    def __get_table_column_names(self):
        raw_attr = self.__DB.execute(
            'select column_name from information_schema.columns where table_name = \'%s\' order by ordinal_position;' % (
                self.__table_name))
        return list(map(lambda x: x[0], raw_attr))


    def __enumerate_insight(self, S: Subspace, subspace_id: int, Ce: ComponentExtractor, heap: Heap):
        """

        :param S:
        :type S: Subspace
        :param subspace_id:
        :type subspace_id: int
        :param Ce:
        :type Ce: ComponentExtractor
        :param heap:
        :type heap: Heap
        """
        local_heap = Heap(heap.capacity)
        SG = self.__generate_sibling_group(S, subspace_id)

        # phase I
        if self.__is_valid(SG, Ce):
            phi = self.__extract_phi(SG, Ce)
            for _, insight_type in enumerate(InsightType):
                score = self.__imp(SG) * self.__sig(phi, insight_type)
                if score > local_heap.get_max().score:
                    new_Ce = Ce.deepcopy()
                    new_Ce.score = score
                    new_Ce.insight_type = insight_type
                    new_Ce.SG = SG

                    local_heap.push(new_Ce)
                    heap.push(new_Ce)

        # phase II
        for attr_val in self.__dom[self.__subspace_attr_ids[subspace_id]]: # Di
            S_ = S[:]
            S_[subspace_id] = attr_val.deepcopy()
            for j in range(len(S_)): # Dj
                if S_[j].type == AttributeType.ALL:
                    self.__enumerate_insight(S_, j, Ce, heap)


    def __extract_phi(self, SG: SiblingGroup, Ce: ComponentExtractor) -> OrderedDict[Subspace, Number]:
        """

        :param SG:
        :type SG: SiblingGroup
        :param Ce:
        :type Ce: ComponentExtractor
        :return:
        :rtype: OrderedDict => Subspace: int
        """
        phi = collections.OrderedDict()
        for attr_val in self.__dom[self.__subspace_attr_ids[SG.Di]]:
            S_ = SG.S[:]
            S_[SG.Di] = attr_val.deepcopy()
            M_ = self.__recur_extract(S_, self.__depth - 1, Ce)
            phi[S_] = M_
        return phi


    def __recur_extract(self, S_: Subspace, level: int, Ce: ComponentExtractor) -> Number:
        """

        :param S_:
        :type S_: Subspace
        :param level:
        :type level: int
        :param Ce:
        :type Ce: ComponentExtractor
        :return:
        :rtype: Number
        """
        if level > 1:
            phi_level = collections.OrderedDict()
            D_e, subspace_id = Ce[level].Dx, Ce[level].subspace_id

            for attr_val in self.__dom[D_e]:
                S_v = S_[:]
                S_v[subspace_id] = attr_val.deepcopy()
                M_v = self.__recur_extract(S_v, level - 1, Ce)
                phi_level[S_v] = M_v

            aggregate_type = Ce[level].aggregate_type
            M_ = self.__measurement(aggregate_type, phi_level, S_)

        else:
            M_ = self.__sum(S_)

        return M_


    def __measurement(self, aggregate_type: AggregateType, phi_level: OrderedDict[Subspace, Number], S: Subspace) -> Number:
        """

        :param aggregate_type:
        :type aggregate_type: AggregateType
        :param phi_level:
        :type phi_level: OrderedDict => Subspace: value
        :param S:
        :type S: Subspace
        :return:
        :rtype: float
        """
        aggregate_function = AggregateFunctionFactory.get_aggregate_function(aggregate_type)
        result = aggregate_function.measurement(phi_level)
        return result[S]


    @timer('enumerate all Ce')
    def __enumerate_all_Ce(self) -> List[ComponentExtractor]:
        """

        :return:
        :rtype: List[ComponentExtractor]
        """
        output = [ComponentExtractor.get_default_Ce(self.__measurement_attr_id)]

        for i, attr_id in enumerate(self.__subspace_attr_ids):
            new_output = []
            if i == 0:
                ce = output[0]
                for aggr_type in AggregateType.get_aggregate_types():
                    ce_ = ce.deepcopy()
                    ce_.append(Extractor(aggr_type, attr_id, i))
                    new_output.append(ce_)
            else:
                for ce in output:
                    for aggr_type in self.__adj_extractors[ce[-1].aggregate_type]:
                        ce_ = ce.deepcopy()
                        ce_.append(Extractor(aggr_type, attr_id, i))
                        new_output.append(ce_)

            output = new_output[:]

        return output


    def __generate_sibling_group(self, S: Subspace, subspace_id: int) -> SiblingGroup:
        """

        :param S:
        :type S: Subspace
        :param subspace_id:
        :type subspace_id: int
        :return:
        :rtype: SiblingGroup
        """
        SG = SiblingGroup(S, subspace_id)
        SG.append(Subspace())

        for j, attr_val in enumerate(S):
            if subspace_id == j and S[subspace_id].type == AttributeType.ALL:
                new_SG = SiblingGroup(S, subspace_id)
                for attr_val_k in self.__dom[self.__subspace_attr_ids[subspace_id]]:
                    for S_ in SG:
                        new_SG.append(S_ + Subspace([attr_val_k.deepcopy()]))
                SG = new_SG[:]
            else:
                for S_ in SG:
                    S_.append(attr_val.deepcopy())

        return SG


    def __is_valid(self, SG: SiblingGroup, Ce: ComponentExtractor) -> bool:
        """

        :param SG:
        :type SG: SiblingGroup
        :param Ce:
        :type Ce: ComponentExtractor
        :return:
        :rtype: boolean
        """
        for extractor in Ce:
            if extractor.Dx != self.__measurement_attr_id:
                if not (SG.Di == extractor.subspace_id or SG.S[extractor.subspace_id].type != AttributeType.ALL):
                    return False
        return True


    '''Score function'''
    def __imp(self, SG: SiblingGroup) -> Number:
        """

        :param SG:
        :type SG: SiblingGroup
        :return:
        :rtype: float
        """
        all_start_subspace = Subspace.create_all_start_subspace(self.__subspace_dimension)
        sum_all_start_subspace = self.__sum(all_start_subspace)

        total = 0
        for S_ in SG:
            total += (self.__sum(S_) / sum_all_start_subspace)
        return total


    def __sig(self, phi: OrderedDict[Subspace, Number], insightType: InsightType) -> Number:
        """

        :param phi:
        :type phi: OrderedDict: S: value
        :param insightType:
        :type insightType: InsightType
        :return:
        :rtype: float
        """
        scoring = ScoreCalculatorFactory.get_score_calculator(insightType)
        return scoring.sig(phi)


    '''Aggregate sum'''
    def __sum(self, S: Subspace) -> Number:
        """

        :param S:
        :type S: Subspace
        :return:
        :rtype: Number
        """
        conditions = []
        for subspace_id, attr_val in enumerate(S):
            if attr_val.type != AttributeType.ALL:
                conditions.append((subspace_id, attr_val.value))

        query = "select sum(%s) from %s" % (self.__table_column_names[self.__measurement_attr_id], self.__table_name)

        if len(conditions) > 0:
            query += " where"
            for i , (subspace_id, attr_exact_val) in enumerate(conditions):
                attr_name = self.__table_column_names[self.__subspace_attr_ids[subspace_id]]

                if i == 0:
                    if isinstance(attr_exact_val, str):
                        query += " %s = '%s'" % (attr_name, attr_exact_val.replace("'", "''"))
                    else:
                        query += " %s = %s" % (attr_name, str(attr_exact_val))
                else:
                    if isinstance(attr_exact_val, str):
                        query += " and %s = '%s'" % (attr_name, attr_exact_val.replace("'", "''"))
                    else:
                        query += " and %s = %s" % (attr_name, str(attr_exact_val))

        group_by  = []
        for subspace_id in range(self.__subspace_dimension):
            if S[subspace_id].type != AttributeType.ALL:
                group_by.append(self.__table_column_names[self.__subspace_attr_ids[subspace_id]])

        if len(group_by) > 0:
            query += " group by"
            for i, attr_name in enumerate(group_by):
                if i == 0:
                    query += " %s" % (attr_name)
                else:
                    query += ", %s" % (attr_name)

        query += ";"
        result = self.__DB.execute(query)

        return result[0][0] if len(result) > 0 else 0


