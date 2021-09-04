#! /usr/bin/env python3

from typing import Sequence
from ..pipeline import Pipeline
from ..utils.bases import Asset
from ..utils.config import (
    _DEFAULT_LAG,
)

class Single(Pipeline):
    def init(self):
        self.universe = tuple(self.assets.values())

    def next(self) -> Sequence[Asset]:
        return self.universe

class Rolling(Pipeline):

    def init(self):
        self.build_chain(), self.apply_roll()

    def next(self) -> Sequence[Asset]:
        if self.main.buffer + _DEFAULT_LAG < len(self.main):
            ref_date = self.main[_DEFAULT_LAG]
            while ref_date > self.maturity:
                self.broker.close(self.ticker)
                self.apply_roll()
            return self.universe

    def apply_roll(self):
        if not self.chain:
            msg="Empty chain"
            raise ValueError(msg)
    
        self.ticker = self.chain.pop()
        self.curr = self.assets.get(self.ticker)
        self.universe = [self.curr]
        self.maturity = self.curr.maturity

class Vertice(Pipeline):
    def init(self):
        raise NotImplementedError()
    
    def next(self) -> Sequence[Asset]:
        raise NotImplementedError()

class Ranking(Pipeline):
    def init(self):
        raise NotImplementedError()
    
    def next(self) -> Sequence[Asset]:
        raise NotImplementedError()