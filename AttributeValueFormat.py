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
    def __init__(self, _type: AttributeType, _attr_id: int):
        self._value = None
        self._type = _type
        self._attr_id = _attr_id

    @property
    def type(self):
        return self._type

    @property
    def value(self):
        return self._value

    @property
    def attribute_id(self):
        return self._attr_id

    def deepcopy(self) -> AttributeValue:
        return copy.deepcopy(self)

    @abstractmethod
    def __eq__(self, other: AttributeValue) -> bool:
        raise NotImplemented('need to implement')

    def __str__(self) -> str:
        return str(self._value)

    def __repr__(self) -> str:
        return str(self._value)


class AttributeValueAll(AttributeValue):
    def __init__(self, _attr_id: int):
        super().__init__(AttributeType.ALL, _attr_id)
        self._value = '*'

    def __eq__(self, other: AttributeValue) -> bool:
        if other.type == AttributeType.ALL:
            return True
        return False


class AttributeValueString(AttributeValue):
    def __init__(self, _value: str, _attr_id: int):
        super().__init__(AttributeType.STRING, _attr_id)
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
    def __init__(self, _value: Number, _attr_id: int):
        super().__init__(AttributeType.NUMBER, _attr_id)
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
    def get_attribute_value(value: NumberString, attribute_id: int) -> AttributeValue:
        if isinstance(value, int) or isinstance(value, float):
            return AttributeValueNumber(value, attribute_id)
        elif isinstance(value, str):
            if value == '*':
                return AttributeValueAll(attribute_id)
            return AttributeValueString(value, attribute_id)
        else:
            return None


