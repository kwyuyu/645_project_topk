import heapq
from typing import *
from CustomType import *

class MaxHeap(object):
    def __init__(self, _capacity: int):
        self._capacity = _capacity
        self.H = []

    @property
    def capacity(self) -> int:
        return self._capacity

    def push(self, value: Number):
        heapq.heappush(self.H, -value)
        if len(self.H) > self._capacity:
            heapq.heappop(self.H)

    def pop(self) -> Number:
        if len(self.H) == 0:
            return None
        return -heapq.heappop(self.H)

    def extract_max(self) -> Number:
        if len(self.H) == 0:
            return -float('inf')
        return -self.H[0]

    def get_sorted_output(self, increasing = True) -> List:
        tmp_H = self.H[:]
        output = []
        while tmp_H:
            output.append(-heapq.heappop(tmp_H))
        return output if not increasing else output[::-1]



