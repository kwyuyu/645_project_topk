from __future__ import annotations

import Heap
import collections
from typing import *

from Utils import *
from ScoreFunction import *
from AggregateFunctionFormat import *
from AttributeValueFormat import *
from DatabaseOperation import *


if TYPE_CHECKING:
    from CustomizedType import *


class TopKInsight(object):
    def __init__(self, DB: Database):
        """

        :param DB:
        :type DB: Database
        """
        self.adj_extractors = {AggregateType.RANK: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV],
                               AggregateType.PERCENTILE: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV],
                               AggregateType.DELTA_AVG: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV],
                               AggregateType.DELTA_PREV: [AggregateType.RANK, AggregateType.DELTA_AVG, AggregateType.DELTA_PREV]}

        self.DB = DB
        self.table_name = 'temp'

        self.dom = dict() # Di: list() of AttributeValue

        self.table_dimension = 0
        self.d = 0

        self.depth = 0

        self.attr_id = []
        self.attr_name = []

        self.D = []
        self.M = -1

        self.__generate_dom(self.table_name)





    '''Initialization'''
    def __generate_dom(self, table_name: str):
        """

		:param table_name:
		:type table_name: str
		"""
        # get table attribute name
        raw_attr = self.DB.execute(
            'select column_name from information_schema.columns where table_name = \'%s\' order by ordinal_position;' % (
                table_name))
        self.table_dimension = len(raw_attr)
        self.d = self.table_dimension - 1

        for i, attr_tuple in enumerate(raw_attr):
            self.attr_id.append(i)
            self.attr_name.append(attr_tuple[0])

            self.dom[i] = list()

            raw_output = self.DB.execute('select distinct %s from %s;' % (attr_tuple[0], table_name))
            for raw_tuple in raw_output:
                self.dom[i].append(AttributeValueFactory.get_attribute_value(raw_tuple[0]))

    def __set_measurement(self, measurement_id: int):
        self.M = measurement_id
        self.D = self.attr_id[:measurement_id] + self.attr_id[measurement_id + 1:]




    '''Main algorithm'''
    def insghts(self, result_size: int, insight_dimension: List[int]) -> List[ComponentExtractor]:
        """

        :param result_size:
        :type result_size: int
        :param insight_dimension: the dimension that each Extractor used to measure.
        :type insight_dimension: List[int], ex. [measurement, D0, D1, D2, ...]
        :return:
        :rtype: sorted List[ComponentExtractor]
        """
        self.__set_measurement(insight_dimension[0])
        self.depth = len(insight_dimension)

        heap = Heap.Heap(result_size)
        possible_Ce = self.__enumerate_all_Ce(self.depth, insight_dimension)

        for Ce in possible_Ce:
            for i in range(self.table_dimension):
                if i != self.M:
                    S = Subspace.create_all_start_subspace(self.table_dimension, self.M)
                    self.__enumerate_insight(S, i, Ce, heap)

        return heap.get_nlargest()


    def __enumerate_insight(self, S: Subspace, i: int, Ce: ComponentExtractor, heap: Heap):
        """

        :param S:
        :type S: Subspace
        :param i:
        :type i: int
        :param Ce:
        :type Ce: ComponenetExtractor
        :param heap:
        :type heap: Heap
        """
        local_heap = Heap.Heap(heap.capacity)
        SG = self.__generate_sibling_group(S, i)

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
        for attr_val in self.dom[i]: # Di
            S_ = S[:]
            S_[i] = attr_val.deepcopy()
            for j in range(len(S_)): # Dj
                if S_[j].type == AttributeType.ALL:
                    self.__enumerate_insight(S_, j, Ce, heap)


    def __extract_phi(self, SG: SiblingGroup, Ce: ComponentExtractor) -> OrderedDict[Subspace, Number]:
        """

        :param SG:
        :type SG: SiblingGroup
        :param Ce:
        :type Ce: CompomentExtractor
        :return:
        :rtype: OrderedDict => Subspace: int
        """
        phi = collections.OrderedDict()
        for attr_val in self.dom[SG.Di]:
            S_ = SG.S[:]
            S_[SG.Di] = attr_val.deepcopy()
            M_ = self.__recur_extract(S_, self.depth-1, Ce)
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
            D_e = Ce[level].Dx

            for attr_val in self.dom[D_e]:
                S_v = S_[:]
                S_v[D_e] = attr_val.deepcopy()
                M_v = self.__recur_extract(S_v, level - 1, Ce)
                phi_level[S_v] = M_v

            aggregate_type = Ce[level].aggregate_function
            M_ = self.__measurement(aggregate_type, phi_level, S_)

        else:
            M_ = self.__sum(S_)

        return M_


    def __measurement(self, aggregate_type: AggregateType, phi_level: OrderedDict[Subspace, Number], S: Subspace) -> Number:
        """

        :param aggregate_type:
        :type aggregate_type: AggregateType
        :param phi_level:
        :type phi_level: OrderdDict => Subspace: value
        :param S:
        :type S: Subspace
        :return:
        :rtype: float
        """
        aggregate_function = AggregateFunctionFactory.get_aggregate_function(aggregate_type)
        result = aggregate_function.measurement(phi_level)
        return result[S]


    def __enumerate_all_Ce(self, depth: int, dimension: List[int]) -> List[ComponentExtractor]:
        """

        :param depth:
        :type depth: List[int]
        :param dimension: the dimension that each Extractor used to measure
        :type dimension: List[int]
        :return:
        :rtype: List[ComponentExtractor]
        """
        output = [ComponentExtractor.get_default_Ce(dimension[0])]

        for i in range(1, depth):
            new_output = []
            if i == 1:
                ce = output[0]
                for aggr_type in AggregateType.get_aggregate_types():
                    ce_ = ce.deepcopy()
                    ce_.append(Extractor(aggr_type, dimension[i]))
                    new_output.append(ce_)
            else:
                for ce in output:
                    for aggr_type in self.adj_extractors[ce[-1].aggregate_type]:
                        ce_ = ce.deepcopy()
                        ce_.append(Extractor(aggr_type, dimension[i]))
                        new_output.append(ce_)

            output = new_output[:]

        return output


    def __generate_sibling_group(self, S: Subspace, i: int) -> SiblingGroup:
        """

        :param S:
        :type S: Subspace
        :param i:
        :type i: int
        :return:
        :rtype: SiblingGroup
        """
        SG = SiblingGroup(S, i)
        SG.append(Subspace([]))

        for j, attr_val in enumerate(S):
            if i == j and S[i].type == AttributeType.ALL:
                new_SG = SiblingGroup(S, i)
                for attr_val_k in self.dom[i]:
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
            if not (SG.Di == extractor.Dx or SG.S[extractor.Dx].type != AttributeType.ALL):
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
        all_start_subspace = Subspace.create_all_start_subspace(self.table_dimension, self.M)
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
        for i, attr_val in enumerate(S):
            if attr_val.type != AttributeType.ALL and attr_val.type != AttributeType.NONE:
                conditions.append((i, attr_val.value))

        query = 'select sum(%s) from %s' % (self.attr_name[self.M], self.table_name)

        if len(conditions) > 0:
            query += ' where'
            for i , (attr_id, attr_exact_val) in enumerate(conditions):
                if i == 0:
                    if isinstance(attr_exact_val, str):
                        query += ' %s = \'%s\'' % (self.attr_name[attr_id], attr_exact_val)
                    else:
                        query += ' %s = %s' % (self.attr_name[attr_id], str(attr_exact_val))
                else:
                    if isinstance(attr_exact_val, str):
                        query += ' and %s = \'%s\'' % (self.attr_name[attr_id], attr_exact_val)
                    else:
                        query += ' and %s = %s' % (self.attr_name[attr_id], str(attr_exact_val))

        group_by  = []
        for i in self.D:
            if S[i].type != AttributeType.ALL:
                group_by.append(self.attr_name[i])

        if len(group_by) > 0:
            query += ' group by'
            for i, attr_name in enumerate(group_by):
                if i == 0:
                    query += ' %s' % (attr_name)
                else:
                    query += ', %s' % (attr_name)

        query += ';'
        result = self.DB.execute(query)

        return result[0][0] if len(result) > 0 else 0


