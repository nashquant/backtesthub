#! /usr/bin/env python3

from ..pipeline import Pipeline
from typing import Sequence
from ..utils.bases import Asset, Hedge
from ..utils.config import (
    _DEFAULT_LAG,
)


class Default(Pipeline):
    def init(self):
        self.universe = tuple(self.assets.values())

    def run(self) -> Sequence[Asset, Hedge]:
        return self.universe


class Rolling(Pipeline):

    def init(self):
        self.chain = self.build_chain()
        ticker = self.chain.pop()
        self.asset = self.assets[ticker]
        self.universe = [self.asset]
        self.maturity = self.asset.maturity

    def run(self) -> Sequence[Asset, Hedge]: 
        if self.asset.index[_DEFAULT_LAG] > self.maturity:
            if not self.chain:
                msg="Cannot derive any assets from empty chain"
                raise ValueError(msg)

            ticker = self.chain.pop()
            self.asset = self.assets[ticker]
            self.universe = [self.asset]
            self.maturity = self.asset.maturity
        
        return self.universe

class Vertice(Pipeline):
    def init(self):
        raise NotImplementedError()
    
    def run(self) -> Sequence[Asset, Hedge]:
        raise NotImplementedError()

class Ranking(Pipeline):
    def init(self):
        raise NotImplementedError()
    
    def run(self) -> Sequence[Asset, Hedge]:
        raise NotImplementedError()