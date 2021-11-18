#! /usr/bin/env python3

from datetime import date
from operator import itemgetter
from collections import OrderedDict
from abc import ABCMeta, abstractmethod
from workdays import workday
from typing import Dict, Sequence
from .utils.bases import Line, Asset
from .broker import Broker
from .utils.config import (
    _DEFAULT_LAG,
)


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
        holidays: Sequence[date] = [],
        assets: Dict[str, Asset] = OrderedDict(),
        hedges: Dict[str, Asset] = OrderedDict(),
    ):
        self.__main = main
        self.__broker = broker
        self.__assets = assets
        self.__hedges = hedges
        self.__holidays = holidays

        self.__universe = []

    @abstractmethod
    def init(self):
        """
        `Pipeline Initialization`

        Since this is an abstract method, it 
        is expected to be overriden by another 
        method belonging to a child class.

        This child class' init method will be
        responsible for setting up the initial
        conditions of the pipeline object.
        """

    @abstractmethod
    def next(self) -> Sequence[Asset]:
        """ 
        `Pipeline Running`

        Since this is an abstract method, it 
        is expected to be overriden by another 
        method belonging to a child class.

        This child class' next method will be
        responsible for determining a sequence
        of tradeable `Assets` (data type) that
        are allowed to be traded at some date,
        which is a.k.a `universe`.

        Called first time @ `start date` + `buffer`
        and recurrently called at each period after
        as defined by the `global index` set up
        until it reaches end or the simulation is
        stopped.

        Refer to backtesthub/calendar.py to know
        more about global index setting.

        NOTE: We assume the Pipeline's next method 
        to be responsible to close opened positions
        that no longer remains in the universe.         
        """

    def build_chain(self):
        """
        `Build Chain Method`

        This method is very important for
        futures because it concatenates
        cronologically the futures by
        maturity.

        It is important because futures
        pipelines uses that to define
        priority.
        """

        maturities: Dict[str, date] = {
            asset.ticker: asset.maturity
            for asset in self.assets.values()
            if asset.maturity is not None
        }

        chain: Dict[str, date] = dict(
            sorted(
                maturities.items(),
                key=itemgetter(1),
                reverse=True,
            )
        )

        self.chain = [self.assets.get(tk) for tk in chain.keys()]

    def __repr__(self):
        return f"{self.__class__.__name__}<Universe: {self.universe}>"

    def get_lagged_date(self, lag: int = _DEFAULT_LAG) -> date:
        """
        `Get Date Method`

        This function is an utils for users that
        want to work with date management given
        that `self.__main` holds the global index.

        """

        return workday(self.__main[0], lag, self.__holidays)

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

    @property
    def date(self) -> date:
        return self.main[0]
