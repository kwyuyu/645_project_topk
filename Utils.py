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
            self.__subspace = []
        else:
            self.__subspace = _subspace

    @staticmethod
    def create_all_start_subspace(size: int) -> Subspace:
        subspace = [AttributeValueFactory.get_attribute_value('*')] * size
        return Subspace(subspace)

    @property
    def subspace(self):
        return self.__subspace

    def append(self, value: AttributeValue):
        self.__subspace.append(value)

    def deepcopy(self) -> Subspace:
        return copy.deepcopy(self)

    def __len__(self) -> int:
        return len(self.__subspace)

    def __eq__(self, other: Subspace) -> bool:
        return self.__subspace == other.subspace

    def __iadd__(self, other: Subspace) -> Subspace:
        return Subspace(self.__subspace + other.subspace)

    def __add__(self, other: Subspace) -> Subspace:
        return Subspace(self.__subspace + other.subspace)

    def __getitem__(self, key: GetItemKey) -> SubspaceTypes:
        if isinstance(key, int):
            return self.__subspace[key]
        return Subspace(self.__subspace[key])

    def __setitem__(self, key: int, value: AttributeValue):
        self.__subspace[key] = value

    def __iter__(self) -> Generator[AttributeValue]:
        for attr_val in self.__subspace:
            yield attr_val

    def __hash__(self):
        output = []
        for attr_val in self.__subspace:
            output.append(attr_val.value)
        return hash(tuple(output))

    def __repr__(self) -> str:
        return self.__subspace.__repr__()


class SiblingGroup(object):
    def __init__(self, S: Subspace, subspace_id: int, sibling_subspace: List[Subspace] = None):
        """
        SG(S, Di): a list of Subspace
        :param S:
        :type S: Subspace
        :param subspace_id:
        :type subspace_id: int
        :param sibling_subspace:
        :type sibling_subspace: list of AttributeValue
        """
        self.__S = S
        self.__Di = subspace_id

        if sibling_subspace is None:
            self.__sibling_subspace = []
        else:
            self.__sibling_subspace = sibling_subspace

    @property
    def sibling_subspace(self):
        return self.__sibling_subspace

    @property
    def S(self):
        return self.__S

    @property
    def Di(self):
        return self.__Di
    
    def append(self, subspace: Subspace):
        self.__sibling_subspace.append(subspace)

    def deepcopy(self):
        return copy.deepcopy(self)

    def __len__(self) -> int:
        return len(self.__sibling_subspace)
        
    def __getitem__(self, key: GetItemKey) -> SiblingGroupTypes:
        if isinstance(key, int):
            return self.__sibling_subspace[key]
        return SiblingGroup(self.__S.deepcopy(), self.__Di, self.__sibling_subspace[key])

    def __setitem__(self, key: slice, value: Subspace):
        self.__sibling_subspace[key] = value

    def __iter__(self) -> Generator[Subspace]:
        for subspace in self.__sibling_subspace:
            yield subspace

    def __repr__(self) -> str:
        return self.__sibling_subspace.__repr__()


class Extractor(object):
    def __init__(self, aggregate_function: AggregateType, measure_attribute_id: int, subspace_id: int):
        """
        Ce
        :param aggregate_function:
        :type aggregate_function: AggregateType
        :param measure_attribute_id: attribute dimension (index)
        :type measure_attribute_id: int
        :param subspace_id: the index in Subspace that measurement_attribute is located, -1 represents main measurement
        :type subspace_id: int
        """
        self.__aggregate_function = aggregate_function
        self.__measure_attribute_id = measure_attribute_id
        self.__subspace_id = subspace_id

    @property
    def aggregate_type(self) -> AggregateType:
        return self.__aggregate_function

    @property
    def Dx(self) -> int:
        return self.__measure_attribute_id

    @property
    def subspace_id(self):
        return self.__subspace_id

    def deepcopy(self) -> Extractor:
        return copy.deepcopy(self)

    def __repr__(self) -> str:
        return str((self.aggregate_type.name, self.__measure_attribute_id))


class ComponentExtractor(object):
    def __init__(self, _Ce: List[Extractor] = None):
        """

        :param _Ce:
        :type _Ce: list of Extractor
        """
        self.__insight_type = None
        self.__SG = None
        self.__score = -float('inf')

        if _Ce is None:
            self.__Ce = []
        else:
            self.__Ce = _Ce

    @staticmethod
    def get_default_Ce(measurement_attribute):
        return ComponentExtractor([Extractor(AggregateType.SUM, measurement_attribute, -1)])

    @property
    def Ce(self) -> List[Extractor]:
        return self.__Ce

    @property
    def score(self) -> Number:
        return self.__score

    @score.setter
    def score(self, _score: Number):
        if not isinstance(_score, int) and not isinstance(_score, float):
            raise TypeError(f'score should be number. input = {_score}, {type(_score)}')
        self.__score = _score

    @property
    def insight_type(self) -> InsightType:
        return self.__insight_type

    @insight_type.setter
    def insight_type(self, _insight_type: InsightType):
        if not isinstance(_insight_type, InsightType):
            raise TypeError('insight type error')
        self.__insight_type = _insight_type

    @property
    def SG(self) -> SiblingGroup:
        return self.__SG

    @SG.setter
    def SG(self, _SG: SiblingGroup):
        if not isinstance(_SG, SiblingGroup):
            raise TypeError('sibling group type error')
        self.__SG = _SG

    def deepcopy(self) -> ComponentExtractor:
        return copy.deepcopy(self)

    def append(self, extractor: Extractor):
        self.__Ce.append(extractor)

    def __lt__(self, other: ComponentExtractor) -> bool:
        return self.score < other.score

    def __len__(self) -> int:
        return len(self.__Ce)

    def __getitem__(self, key: GetItemKey) -> ComponentExtractorTypes:
        if isinstance(key, int):
            return self.__Ce[key]
        return ComponentExtractor(self.__Ce[key])

    def __iter__(self) -> Generator[Extractor]:
        for extractor in self.__Ce:
            yield extractor

    def __repr__(self) -> str:
        return str((self.score,
                    self.insight_type.name if self.insight_type else None,
                    self.SG.S if self.SG else None,
                    self.SG if self.SG else None,
                    self.__Ce))
