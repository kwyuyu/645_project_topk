from __future__ import annotations

import enum
import copy
from typing import *
from abc import *

if TYPE_CHECKING:
    from CustomizedType import *


'''Enum'''
class AttributeType(enum.Enum):
    ALL = 0
    NUMBER = 1
    STRING = 2


'''Abstract'''
class AttributeValue(ABC):
    def __init__(self, _type: AttributeType):
        self._value = None
        self._type = _type

    @property
    def type(self):
        return self._type

    @property
    def value(self):
        return self._value

    def deepcopy(self) -> AttributeValue:
        return copy.deepcopy(self)

    @abstractmethod
    def __eq__(self, other: AttributeValue) -> bool:
        raise NotImplemented('need to implement')

    def __repr__(self) -> str:
        return str(self._value)


class AttributeValueAll(AttributeValue):
    def __init__(self):
        super().__init__(AttributeType.ALL)
        self._value = '*'

    def __eq__(self, other: AttributeValue) -> bool:
        if other.type == AttributeType.ALL:
            return True
        return False


class AttributeValueString(AttributeValue):
    def __init__(self, _value: str):
        super().__init__(AttributeType.STRING)
        self._value = _value

    @AttributeValue.value.setter
    def value(self, val: str):
        if not isinstance(val, str):
            raise TypeError('Type should be str')
        self._value = val

    def __eq__(self, other: AttributeValue) -> bool:
        if other.type == AttributeType.STRING:
            return self.value == other.value
        return False


class AttributeValueNumber(AttributeValue):
    def __init__(self, _value: Number):
        super().__init__(AttributeType.NUMBER)
        self._value = _value

    @AttributeValue.value.setter
    def value(self, val: Number):
        if not isinstance(val, int) and not isinstance(val, float):
            raise TypeError('Type should be number')
        self._value = val

    def __eq__(self, other: AttributeValue) -> bool:
        if other.type == AttributeType.NUMBER:
            return self.value == other.value
        return False





'''Factory'''
class AttributeValueFactory(object):
    @staticmethod
    def get_attribute_value(value: NumberString) -> AttributeValue:
        if isinstance(value, int) or isinstance(value, float):
            return AttributeValueNumber(value)
        elif isinstance(value, str):
            if value == '*':
                return AttributeValueAll()
            return AttributeValueString(value)
        else:
            return None


