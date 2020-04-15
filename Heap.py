from __future__ import annotations

import heapq
from typing import *

from Utils import *


class Heap(object):
    def __init__(self, _capacity: int):
        """

        :param _capacity: int
        """
        self._capacity = _capacity
        self.H = []
        self.max_value = ComponentExtractor([])
        self.max_value.score = -float('inf')

    @property
    def capacity(self) -> int:
        return self._capacity

    def push(self, value: ComponentExtractor):
        self.max_value = max(self.max_value, value)
        heapq.heappush(self.H, value)
        if len(self.H) > self._capacity:
            heapq.heappop(self.H)

    def pop(self) -> ComponentExtractor:
        if len(self.H) == 0:
            return None
        return heapq.heappop(self.H)

    def get_max(self) -> ComponentExtractor:
        return self.max_value

    def get_min(self) -> ComponentExtractor:
        if len(self.H) == 0:
            return None
        return self.H[0]

    def get_nlargest(self) -> List[ComponentExtractor]:
        return heapq.nlargest(self._capacity, self.H)

    def __repr__(self) -> str:
        return str(self.H)



