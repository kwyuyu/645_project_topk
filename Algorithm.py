import Heap
import collections
import re
from Utils import *
from ScoreFunction import *
from AggregateFunction import *


class TopKInsight(object):
    def __init__(self, DB):
        """

        :param DB:
        :type DB: Database
        """
        self.DB = DB
        self.dom = dict() # Di: list() of AttributeValue
        self.table_dimension = 0
        self.d = 0
        self.attr_id = []
        self.attr_name = []

        self._generate_dom('allpaperauths')

    def _generate_dom(self, table_name):
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
                self.dom[i].append(AttributeValue(raw_tuple[0]))


    # TODO: return type need more detail
    def insghts(self, depth, result_size):
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
            S = Subspace([AttributeValue()] * d)
            self._enumerate_insight(S, i, Ce, heap)

        return heap.get_sorted_output()


    def _enumerate_insight(self, S, i, Ce, heap):
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
                if S_[j] == '*':
                    self._enumerate_insigh(S_, j, Ce, heap)


    def _extract_phi(self, SG, Ce, S, i):
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
        :rtype: OrderedDict
        """
        phi = collections.OrderedDict()
        for val in self.dom[i]:
            S_ = S[:]
            S_[i] = val
            M_ = self._recur_extract(S_, depth, Ce)
            phi[S] = M_
        return phi


    # TODO: SUM function? how to implement it? use it as aggregate function ?
    def _recur_extract(self, S_, level, Ce):
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

            for val in self.dom[D_e]:
                S_v = _S[:]
                S_v[D_E] = val
                M_v = self._recur_extract(S_v, level - 1, Ce)
                phi_level[S_v] = M_v

            E = Ce[level].aggregate_function
            M_ = self._measurement(E, phi_level, S_)

        else:
            M_ = SUM(S_)

        return M_


    def _measurement(self, E, phi_level, S):
        """

        :param E:
        :type E: AggregateType
        :param phi_level:
        :type phi_level: int
        :param S:
        :type S: Subspace
        :return:
        :rtype: float
        """
        aggregate_function = AggregateFunctionFactory.get_aggregate_function(E)
        result = aggregate_function.measurement(phi)
        return result[S]


    # TODO: enumerate all Ce
    def _enumerate_all_Ce(self, depth):
        """

        :param depth:
        :type depth: int
        :return:
        :rtype: list of Ce
        """
        raise Exception('Need to implement')


    def _generate_sibling_group(self, S, i):
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

        for j, val in enumerate(S):
            if i == j and S[i] == '*':
                new_SG = SiblingGroup(S, i)
                for k in self.dom[i]:
                    for S_ in SG:
                        new_SG.append(S_ + Subspace([AttributeValue(k)]))
                SG = new_SG[:]
            else:
                for S_ in SG:
                    S_.append(val)

        return SG


    def _is_valid(self, SG, Ce):
        """

        :param SG:
        :type SG: SiblingGroup
        :param Ce:
        :type Ce: list of Extractor
        :return:
        :rtype: boolean
        """
        return SG.Di == Ce.Dx or SG.S[Ce.Dx] != '*'



    # TODO: imp function
    def _imp(self, SG):
        """

        :param SG:
        :type SG: SiblingGroup
        :return:
        :rtype: float
        """
        total = 0
        for S_ in SG:
            pass
            # SUM(S_) / SUM(<*, * , *>)
        return total

    def _sig(self, phi, insightType):
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

