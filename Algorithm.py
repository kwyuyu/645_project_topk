import Heap
import collections
from typing import *
from CustomType import *
from Utils import *
from ScoreFunction import *
from AggregateFunctionFormat import *
from AggregateFunctionFormat import *
from DatabaseOperation import *


class TopKInsight(object):
    def __init__(self, DB: Database):
        """

        :param DB:
        :type DB: Database
        """
        self.DB = DB
        self.table_name = 'allpaperauths'

        self.dom = dict() # Di: list() of AttributeValue

        self.table_dimension = 0
        self.d = 0

        self.attr_id = []
        self.attr_name = []

        self.D = []
        self.M = -1

        self._generate_dom(self.table_name)
        self._set_measurement(1)


    '''Initialization'''
    def _generate_dom(self, table_name: str):
        """

        :param table_name:
        :type table_name: str
        """
        # get table attribute name
        raw_attr = self.DB.execute('select column_name from information_schema.columns where table_name = \'%s\' order by ordinal_position;' % (table_name))
        self.table_dimension = len(raw_attr)

        for i, attr_tuple in enumerate(raw_attr):
            self.attr_id.append(i)
            self.attr_name.append(attr_tuple[0])

            self.dom[i] = list()

            raw_output = self.DB.execute('select distinct %s from %s;' % (attr_tuple[0], table_name))
            for raw_tuple in raw_output:
                self.dom[i].append(AttributeValueFactory.get_attribute_value(raw_tuple[0]))


    def _set_measurement(self, measurement_id: int):
        self.M = measurement_id
        self.D = self.attr_id[:measurement_id] + self.attr_id[measurement_id + 1:]




    '''Main algorithm'''
    # TODO: return type need more detail
    def insghts(self, depth: int, result_size: int) -> list:
        """

        :param depth:
        :type depth: int
        :param result_size:
        :type result_size: int
        :return:
        :rtype: ??? list of sorted score? list of query by sorted score?
        """
        # R(D, M)
        # dimension of D
        self.d = self.table_dimension - 1

        heap = Heap.MaxHeap(result_size)
        possible_Ce = self._enumerate_all_Ce(depth)

        for Ce in possible_Ce:
            S = Subspace.create_all_start_subspace(self.table_dimension, self.M)
            self._enumerate_insight(S, i, Ce, heap)

        return heap.get_sorted_output()


    def _enumerate_insight(self, S: Subspace, i: int, Ce: List[Extractor], heap: Heap):
        """

        :param S:
        :type S: Subspace
        :param i:
        :type i: int
        :param Ce:
        :type Ce: list of Extractor
        :param heap:
        :type heap: MaxHeap
        """
        local_heap = Heap.MaxHeap(heap.capacity)
        SG = self._generate_sibling_group(S, i)

        # phase I
        if self._is_valid(SG, Ce):
            phi = self._extract_phi(SG, Ce, S, i)
            for _, insight_type in enumerate(InsightType):
                score = self._imp(SG) * self._sig(phi, insight_type)

                if score > local_heap.extract_max():
                    local_heap.push(score)
                    heap.push(score)

        # phase II
        for val in self.dom[i]:  # Di
            S_ = S[:]
            S_[i] = val
            for j in S_: # Dk
                if S_[j].type == AttributeType.ALL:
                    self._enumerate_insigh(S_, j, Ce, heap)


    def _extract_phi(self, SG: SiblingGroup, Ce: list, S: Subspace, i: int) -> OrderedDict[Subspace, Number]:
        """

        :param SG:
        :type SG: SiblingGroup
        :param Ce:
        :type Ce: list of Extractor
        :param S:
        :type S: Subspace
        :param i:
        :type i: int
        :return:
        :rtype: OrderedDict => Subspace: int
        """
        phi = collections.OrderedDict()
        for attr_val in self.dom[i]:
            S_ = S[:]
            S_[i] = attr_val
            M_ = self._recur_extract(S_, depth, Ce)
            phi[S] = M_
        return phi


    def _recur_extract(self, S_: Subspace, level: int, Ce: List[Extractor]) -> Number:
        """

        :param S_:
        :type S_: Subspace
        :param level:
        :type level: int
        :param Ce:
        :type Ce: list of Extractor
        :return:
        :rtype: float
        """
        if level > 1:
            phi_level = collections.OrderedDict()
            D_e = Ce[level].Dx

            for attr_val in self.dom[D_e]:
                S_v = _S[:]
                S_v[D_E] = attr_val
                M_v = self._recur_extract(S_v, level - 1, Ce)
                phi_level[S_v] = M_v

            aggregate_type = Ce[level].aggregate_function
            M_ = self._measurement(aggregate_type, phi_level, S_)

        else:
            M_ = self._sum(S_)

        return M_


    def _measurement(self, aggregate_type: AggregateType, phi_level: OrderedDict[Subspace, Number], S: Subspace) -> Number:
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


    # TODO: enumerate all Ce
    def _enumerate_all_Ce(self, depth: int) -> List[List[Extractor]]:
        """

        :param depth:
        :type depth: int
        :return:
        :rtype: list of Ce
        """
        raise NotImplemented('Need to implement')


    def _generate_sibling_group(self, S: Subspace, i: int) -> SiblingGroup:
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
                        new_SG.append(S_ + Subspace([attr_val_k]))
                SG = new_SG[:]
            else:
                for S_ in SG:
                    S_.append(attr_val)

        return SG


    def _is_valid(self, SG: SiblingGroup, Ce: List[Extractor]) -> bool:
        """

        :param SG:
        :type SG: SiblingGroup
        :param Ce:
        :type Ce: list of Extractor
        :return:
        :rtype: boolean
        """
        for extractor in Ce:
            if not (SG.Di == extractor.Dx or SG.S[extractor.Dx].type != AttributeType.ALL):
                return False
        return True


    '''Score function'''
    def _imp(self, SG: SiblingGroup) -> Number:
        """

        :param SG:
        :type SG: SiblingGroup
        :return:
        :rtype: float
        """
        all_start_subspace = Subspace.create_all_start_subspace(self.table_dimension, self.M)
        sum_all_start_subspace = self._sum(all_start_subspace)

        total = 0
        for S_ in SG:
            total += (self._sum(S_) / sum_all_start_subspace)
        return total


    def _sig(self, phi: OrderedDict[Subspace, Number], insightType: InsightType) -> Number:
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
    def _sum(self, S: Subspace) -> Number:
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

        query = 'select sum(%s) from %s where' % (self.attr_name[self.M], self.table_name)
        for i , (attr_id, attr_exact_val) in enumerate(conditions):
            if i == 0:
                query += ' %s = %s' % (self.attr_name[attr_id], attr_exact_val)
            else:
                query += ' and %s = %s' % (self.attr_name[attr_id], attr_exact_val)
        query += ' group by'

        group_by = list(map(lambda idx: self.attr_name[idx], self.D))
        for i, attr_name in enumerate(group_by):
            if i == 0:
                query += ' %s'
            else:
                query += ', %s'
        query += ';'

        result = self.DB.execute(query)
        return result[0][0]


