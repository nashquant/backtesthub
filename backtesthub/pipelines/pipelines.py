#! /usr/bin/env python3

from ..pipeline import Pipeline

from ..utils.config import (
    _DEFAULT_LAG,
)


class Default(Pipeline):
    def init(self):
        self.universe = tuple(self.assets.values())

    def run(self):
        return self.universe


class Rolling(Pipeline):

    def init(self):
        self.chain = self.build_chain()
        ticker = self.chain.pop(0)
        self.asset = self.assets[ticker]
        self.universe = [self.asset]
        self.maturity = self.asset.maturity

    def run(self):
        if self.asset.__index[_DEFAULT_LAG] >= self.maturity:
            ticker = self.chain.pop(0)
            self.asset = self.assets[ticker]
            self.universe = [self.asset]
            self.maturity = self.asset.maturity
        
        return self.universe

class Vertice(Pipeline):
    def init(self):
        raise NotImplementedError()
    
    def run(self):
        raise NotImplementedError()

class Ranking(Pipeline):
    def init(self):
        raise NotImplementedError()
    
    def run(self):
        raise NotImplementedError()