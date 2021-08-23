#! /usr/bin/env python3

from ..pipeline import Pipeline
from workdays import workday
from datetime import date

from typing import Sequence
from ..utils.config import (
    _DEFAULT_LAG,
)


class Default(Pipeline):
    def init(self):
        self.universe = tuple(self.assets.values())

    def next(self):
        return self.universe


class Rolling(Pipeline):

    def init(self):
        self.chain = self.build_chain()
        ticker = self.chain.pop(0)
        self.asset = self.assets[ticker]
        self.universe = [self.asset]
        self.maturity = self.asset.maturity

    def next(self):
        
        if self.asset.__index[_DEFAULT_LAG] >= self.maturity:
            ticker = self.chain.pop(0)
            self.asset = self.assets[ticker]
            self.universe = [self.asset]
            self.maturity = self.asset.maturity

class Vertice(Pipeline):
    raise NotImplementedError()

class Ranking(Pipeline):
    raise NotImplementedError()