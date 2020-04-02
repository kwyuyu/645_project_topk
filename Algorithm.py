import Heap
from Utils import *

class TopKInsight(object):
    def __init__(self, DB):
        self.DB = DB
        self.dom = dict() # Di: set() of AttributeValue

    def insghts(self, depth, result_size):
        db_attribute_dim = self.DB.get_dimension() # abstrct method, need to implement

        # R(D, M)
        # dimension of D
        d = db_attribute_dim - 1

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
            for _, insight_type in enumerate(InsighType):
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
        :rtype: list of int
        """
        phi = []
        for val in self.dom[i]:
            S_ = S[:]
            S_[i] = val
            M_ = self._recur_extract(S_, depth, Ce)
            phi.extend(M_)
        return phi

    def _recur_extract(self, S_, level, Ce):
        """

        :param S_:
        :type S_: Subspace
        :param level:
        :type level: int
        :param Ce:
        :type Ce: list of Extractor
        """
        if level > 1:
            phi_level = []
            E_e = Ce[level].Dx
        else:
            M_ = SUM(S_)





    def _enumerate_all_Ce(self, depth):
        raise Exception('Need to implement')









    def _generate_sibling_group(self, S, i):
        """

        :param S:
        :type S: Subspace
        :param i:
        :type i: iint
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
        return SG.Di == Ce.Dx or SG.S[Ce.Dx]



    def _imp(self, SG):
        raise Exception('Need to implement')

    def _sig(self, phi, insightType):
        raise Exception('Need to implement')



    def _sum(self, subspace):
        total = 0
        for attri_val in subspace:
            total_val +=