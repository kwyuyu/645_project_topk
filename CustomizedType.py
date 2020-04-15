from __future__ import annotations
from typing import *

from Utils import *
from AttributeValueFormat import *


Number = TypeVar('Number', int, float)
NumberString = TypeVar('NumberString', int, float, str)
SubspaceTypes = TypeVar('SubspaceType', Subspace, AttributeValue)
SiblingGroupTypes = TypeVar('SiblingGroupType', SiblingGroup, Subspace)
ComponentExtractorTypes = TypeVar('ComponentExtractorTypes', ComponentExtractor, Extractor)
GetItemKey = TypeVar('GetItemKey', int , slice)
