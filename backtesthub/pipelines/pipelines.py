#! /usr/bin/env python3

from ..pipeline import Pipeline

class Default(Pipeline):
    
    def init(self):
        self.universe = tuple(self.assets.values())

    def next(self):
        pass