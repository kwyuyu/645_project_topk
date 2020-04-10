from __future__ import annotations

from AttributeValueFormat import *
from typing import *



class Subspace(object):
    def __init__(self, _subspace: List[AttributeValue] = []):
        """
        S: a list of AttributeValue
        :param _subspace:
        :type _subspace: list of AttributeValue
        """
        self._subspace =  _subspace

    @staticmethod
    def create_all_start_subspace(size: int, measurement_dimension: int) -> Subspace:
        subspace = [AttributeValueFactory.get_attribute_value('*')] * size
        subspace[measurement_dimension] = AttributeValueFactory.get_attribute_value('None')
        return subspace

    @property
    def subspace(self):
        return self._subspace

    def append(self, value: AttributeValue):
        self._subspace.append(value)

    def __iadd__(self, other: Subspace) -> SubspaceTypes:
        return Subspace(self._subspace + other.subspace)

    def __add__(self, other: Subspace) -> SubspaceTypes:
        return Subspace(self._subspace + other.subspace)

    def __getitem__(self, key: slice) -> AttributeValue:
        if key.stop is None and key.step is None:
            return self._subspace[key]
        return Subspace(self._subspace[key])

    def __setitem__(self, key: slice, value: AttributeValue):
        self._subspace[key] = value

    def __iter__(self):
        for attr_val in self._subspace:
            yield attr_val

    def __hash__(self):
        output_string = ''
        for attr_val in self._subspace:
            output_string += str(attr_val.value) + ','
        return hash(output_string)

    def __repr__(self):
        return self._subspace.__repr__()


class SiblingGroup(object):
    def __init__(self, S: Subspace, i: int, sibling_attribute: List[Subspace] = []):
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

        self._sibling_attribute = sibling_attribute
    
    def append(self, subspace: Subspace):
        self._sibling_attribute.append(subspace)
        
    def __getitem__(self, key: slice) -> SiblingGroupTypes:
        if key.stop is None and key.step is None:
            return self._sibling_attribute[key]
        return SiblingGroup(self.S, self.i, self._sibling_attribute[key])

    def __setitem__(self, key: slice, value: Subspace):
        self._sibling_attribute[key] = value

    def __iter__(self):
        for subspace in self._sibling_attribute:
            yield subspace

    def __repr__(self):
        return self._sibling_attribute.__repr__()


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
    def Dx(self) -> int:
        return self.measure_attribute


