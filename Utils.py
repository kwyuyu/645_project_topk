from __future__ import annotations

import copy
from typing import *
from AggregateFunctionFormat import *
from AttributeValueFormat import *
from ScoreFunction import *

if TYPE_CHECKING:
    from CustomizedType import *



class Subspace(object):
    def __init__(self, _subspace: List[AttributeValue] = None):
        """
        S: a list of AttributeValue
        :param _subspace:
        :type _subspace: list of AttributeValue
        """
        if _subspace is None:
            self._subspace = []
        else:
            self._subspace = _subspace

    @staticmethod
    def create_all_start_subspace(size: int, measurement_dimension: int) -> Subspace:
        subspace = [AttributeValueFactory.get_attribute_value('*')] * size
        subspace[measurement_dimension] = AttributeValueFactory.get_attribute_value('None')
        return Subspace(subspace)

    @property
    def subspace(self):
        return self._subspace

    def append(self, value: AttributeValue):
        self._subspace.append(value)

    def deepcopy(self) -> Subspace:
        return copy.deepcopy(self)

    def __len__(self) -> int:
        return len(self._subspace)

    def __eq__(self, other: Subspace) -> bool:
        return self._subspace == other.subspace

    def __iadd__(self, other: Subspace) -> Subspace:
        return Subspace(self._subspace + other.subspace)

    def __add__(self, other: Subspace) -> Subspace:
        return Subspace(self._subspace + other.subspace)

    def __getitem__(self, key: GetItemKey) -> SubspaceTypes:
        if isinstance(key, int):
            return self._subspace[key]
        return Subspace(self._subspace[key])

    def __setitem__(self, key: slice, value: AttributeValue):
        self._subspace[key] = value

    def __iter__(self) -> Generator[AttributeValue]:
        for attr_val in self._subspace:
            yield attr_val

    def __hash__(self):
        output = []
        for attr_val in self._subspace:
            output.append(attr_val.value)
        return hash(tuple(output))

    def __repr__(self) -> str:
        return self._subspace.__repr__()


class SiblingGroup(object):
    def __init__(self, S: Subspace, i: int, sibling_subspace: List[Subspace] = None):
        """
        SG(S, Di): a list of Subspace
        :param S:
        :type S: Subspace
        :param i:
        :type i: int
        :param _sibling_attribute:
        :type _sibling_attribute: list of AttributeValue
        """
        self.S = S
        self.Di = i

        if sibling_subspace is None:
            self._sibling_subspace = []
        else:
            self._sibling_subspace = sibling_subspace

    @property
    def sibling_subspace(self):
        return self._sibling_subspace
    
    def append(self, subspace: Subspace):
        self._sibling_subspace.append(subspace)

    def deepcopy(self):
        return copy.deepcopy(self)

    def __len__(self) -> int:
        return len(self._sibling_subspace)
        
    def __getitem__(self, key: GetItemKey) -> SiblingGroupTypes:
        if isinstance(key, int):
            return self._sibling_subspace[key]
        return SiblingGroup(self.S.deepcopy(), self.Di, self._sibling_subspace[key])

    def __setitem__(self, key: slice, value: Subspace):
        self._sibling_subspace[key] = value

    def __iter__(self) -> Generator[Subspace]:
        for subspace in self._sibling_subspace:
            yield subspace

    def __repr__(self) -> str:
        return self._sibling_subspace.__repr__()


class Extractor(object):
    def __init__(self, aggregate_function: AggregateType, measure_attribute: int):
        """
        Ce
        :param aggregate_function:
        :type aggregate_function: AggregateType
        :param measure_attribute: attribute dimension (index)
        :type measure_attribute: int
        """
        self.aggregate_function = aggregate_function
        self.measure_attribute = measure_attribute

    @property
    def aggregate_type(self) -> AggregateType:
        return self.aggregate_function

    @property
    def Dx(self) -> int:
        return self.measure_attribute

    def deepcopy(self) -> Extractor:
        return copy.deepcopy(self)

    def __repr__(self) -> str:
        return str((self.aggregate_function.name, self.measure_attribute))


class ComponentExtractor(object):
    def __init__(self, _Ce: List[Extractor] = None):
        """

        :param _Ce:
        :type _Ce: list of Extractor
        """
        self._insight_type = None
        self._SG = None
        self._score = -float('inf')

        if _Ce is None:
            self._Ce = []
        else:
            self._Ce = _Ce

    @staticmethod
    def get_default_Ce(measurement_attribute):
        return ComponentExtractor([Extractor(AggregateType.SUM, measurement_attribute)])

    @property
    def Ce(self) -> ComponentExtractor:
        return self._Ce

    @property
    def score(self) -> Number:
        return self._score

    @score.setter
    def score(self, _score: Number):
        if not isinstance(_score, int) and not isinstance(_score, float):
            raise TypeError('score should be number')
        self._score = _score

    @property
    def insight_type(self) -> InsightType:
        return self._insight_type

    @insight_type.setter
    def insight_type(self, _insight_type: InsightType):
        if not isinstance(_insight_type, InsightType):
            raise TypeError('insight type error')
        self._insight_type = _insight_type

    @property
    def SG(self) -> SiblingGroup:
        return self._SG

    @SG.setter
    def SG(self, _SG: SiblingGroup):
        if not isinstance(_SG, SiblingGroup):
            raise TypeError('sibling group type error')
        self._SG = _SG

    def deepcopy(self) -> ComponentExtractor:
        return copy.deepcopy(self)

    def append(self, extractor: Extractor):
        self._Ce.append(extractor)

    def __lt__(self, other: ComponentExtractor) -> bool:
        return self.score < other.score

    def __len__(self) -> int:
        return len(self._Ce)

    def __getitem__(self, key: GetItemKey) -> ComponentExtractorTypes:
        if isinstance(key, int):
            return self._Ce[key]
        return ComponentExtractor(self._Ce[key])

    def __iter__(self) -> Generator[Extractor]:
        for extractor in self._Ce:
            yield extractor

    def __repr__(self) -> str:
        return str((self.score, self.insight_type.name, self.SG.S, self._Ce))