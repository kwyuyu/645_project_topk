import enum


class InsightType(enum.Enum):
    POINT = 0
    SHAPE = 1




class AggregateType(enum.Enum):
    SUM = 0
    RANK = 1
    PERCENTILE = 2
    DELTA_AVG = 3
    DELTA_PREV = 4




class AttributeValue(object):
    def __init__(self, _value = '*'):
        """
        attribute value
        :param _value:
        :type _value:
        """
        self._value = _value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):output_phi = dict()
        self._value = val

    def __eq__(self, other):
        if isinstance(other, str) and other == '*':
            return self._value == '*'
        return self._value == other.value

    def __repr__(self):
        return self._value


class Subspace(object):
    def __init__(self, _subspace = []):
        """
        S: a list of AttributeValue
        :param _subspace:
        :type _subspace: list of AttributeValue
        """
        self._subspace =  _subspace

    @property
    def subspace(self):
        return self._subspace

    def append(self, value):
        self._subspace.append(value)

    def __iadd__(self, other):
        return Subspace(self._subspace + other.subspace)

    def __add__(self, other):
        return Subspace(self._subspace + other.subspace)

    def __getitem__(self, key):
        if key.stop is None and key.step is None:
            return self._subspace[key]
        return Subspace(self._subspace[key])

    def __setitem__(self, key, value):
        self._subspace[key] = value

    def __hash__(self):
        output_string = ''
        for attr_val in self._subspace:
            output_string += str(attr_val.value) + ','
        return hash(output_string)

    def __repr__(self):
        return self._subspace.__repr__()


class SiblingGroup(object):
    def __init__(self, S, i, sibling_attribute = []):
        """
        SG(S, Di): a list of Subspace
        :param S:
        :type S:
        :param i:
        :type i:
        :param _sibling_attribute:
        :type _sibling_attribute: list
        """
        self.S = S
        self.Di = i
        self._sibling_attribute = sibling_attribute
    
    def append(self, subspace):
        self._sibling_attribute.append(subspace)
        
    def __getitem__(self, key):
        if key.stop is None and key.step is None:
            return self._sibling_attribute[key]
        return SiblingGroup(self._sibling_attribute[key])

    def __setitem__(self, key, value):
        self._sibling_attribute[key] = value

    def __iter__(self):
        for subspace in self._sibling_attribute:
            yield subspace

    def __repr__(self):
        return self._sibling_attribute.__repr__()

class Extractor(object):
    def __init__(self, aggregate_function, measure_attribute):
        """
        Ce
        :param aggregate_function:
        :type aggregate_function: AggregateType
        :param measure_attribute: attribute dimension
        :type measure_attribute: int
        """
        self.aggregate_function = aggregate_function
        self.measure_attribute = measure_attribute

    @property
    def Dx(self):
        return self.measure_attribute
