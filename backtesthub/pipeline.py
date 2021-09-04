#! /usr/bin/env python3

from datetime import date
from operator import itemgetter
from collections import OrderedDict
from abc import ABCMeta, abstractmethod
from typing import Dict, Sequence, Union
from .utils.bases import Line, Asset
from .broker import Broker


class Pipeline(metaclass=ABCMeta):
    """
    `Pipeline Class`

    Pipeline is responsible for setting up
    the "simulation case" and recursively
    build a sequence called `universe`, which
    provides, for a given date, the list of
    all tradeable assets.

    The `self.universe` attribute may be either
    assigned at `init` if it is supposed to be
    static and defined before the simulation
    begins. Or it can be dynamically defined
    as runs progresses.
    """

    def __init__(
        self,
        main: Line,
        broker: Broker,
        assets: Dict[str, Asset] = OrderedDict(),
        hedges: Dict[str, Asset] = OrderedDict(),
    ):
        self.__main = main
        self.__broker = broker 
        self.__assets = assets
        self.__hedges = hedges
        self.__universe = []

    @abstractmethod
    def init(self):
        """ """

    @abstractmethod
    def next(self) -> Sequence[Asset]:
        """ """

    def build_chain(
        self, 
        assets: Dict[str, Asset]
    ) -> Sequence[str] :

        maturities: Dict[str, date] = {
            asset.ticker: asset.maturity
            for asset in assets.values()
            if asset.maturity is not None
        }

        chain: Dict[str, date] = dict(
            sorted(
                maturities.items(),
                key=itemgetter(1),
                reverse=True,
            )
        )

        return list(chain.keys()) 

    @property
    def asset(self) -> Asset:
        return tuple(self.__assets.values())[0]
    
    @property
    def hedge(self) -> Asset:
        return tuple(self.__hedges.values())[0]
    
    @property
    def main(self) -> Line:
        return self.__main

    @property
    def broker(self) -> Broker:
        return self.__broker

    @property
    def assets(self) -> Dict[str, Asset]:
        return self.__assets
    
    @property
    def hedges(self) -> Dict[str, Asset]:
        return self.__hedges
