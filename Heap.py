import heapq
from typing import *
from CustomizedType import *

class Heap(object):
    def __init__(self, _capacity: int):
        self._capacity = _capacity
        self.H = []
        self.max_value = -float('inf')

    @property
    def capacity(self) -> int:
        return self._capacity

    def push(self, value: Number):
        self.max_value = max(self.max_value, value)
        heapq.heappush(self.H, value)
        if len(self.H) > self._capacity:
            heapq.heappop(self.H)

    def pop(self) -> Number:
        if len(self.H) == 0:
            return None
        return heapq.heappop(self.H)

    def get_max(self) -> Number:
        return self.max_value

    def get_min(self) -> Number:
        if len(self.H) == 0:
            return None
        return self.H[0]

    def get_nlargest(self) -> List:
        return heapq.nlargest(self._capacity, self.H)

    def __repr__(self):
        return str(self.H)



