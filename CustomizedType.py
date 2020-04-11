from typing import *
from Utils import *
from AttributeValueFormat import *


Number = TypeVar('Number', int, float)
SubspaceTypes = TypeVar('SubspaceType', Subspace, AttributeValue)
SiblingGroupTypes = TypeVar('SiblingGroupType', SiblingGroup, Subspace)
ComponentExtractorTypes = TypeVar('ComponentExtractorTypes', ComponentExtractor, Extractor)
GetItemKey = TypeVar('GetItemKey', int , slice)
