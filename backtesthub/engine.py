#! /usr/bin/env python3

import pandas as pd

from datetime import date
from numbers import Number
from collections import OrderedDict
from typing import Dict, Sequence, Union, Optional

from .broker import Broker
from .pipeline import Pipeline
from .strategy import Strategy
from .calendar import Calendar

from .utils.bases import Base, Asset, Hedge

from .utils.config import (
    _DEFAULT_CARRY,
    _DEFAULT_PAIRS,
    _DEFAULT_BUFFER,
)

class Engine:

    """
    `Engine Class`

    Instances of this class are responsible for orchestrating all
    other objects (Strategy, Broker, Position, ...) in order to 
    properly run the simulation.

    It is also responsible for manipulating the global index and
    guaranteeing that all Data/Lines/Broker are synchronized.

    Lots of features such as intraday operations, multi-calendar 
    runs, live trading, etc. are still pending development.

    Some settings may be changed through environment variables 
    configuration. Refer to .utils.config to get more info.
    """

    def __init__(
        self,
        strategy: Strategy,
        pipeline: Pipeline,
        calendar: Calendar,
    ):
        if not issubclass(strategy, Strategy):
            msg = "Arg `strategy` must be a `Strategy` subclass!"
            raise TypeError(msg)
        if not issubclass(pipeline, Pipeline):
            msg = "Arg `pipeline` must be a `Pipeline` subclass!"
            raise TypeError(msg)
        if not isinstance(calendar, Calendar):
            msg = "Arg `calendar` must be a `Calendar` instance!"
            raise TypeError(msg)

        self.__index: Sequence[date] = calendar.index
        self.__bases: Dict[str, Base] = OrderedDict()
        self.__assets: Dict[str, Asset] = OrderedDict()
        self.__hedges: Dict[str, Hedge] = OrderedDict()

        self.__broker: Broker = Broker(
            index=self.__index,
        )

        self.__pipeline: Pipeline = pipeline(
            assets=self.__assets,
            hedges=self.__hedges,
        )

        self.__strategy: Strategy = strategy(
            broker=self.__broker,
            pipeline=self.__pipeline,
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
        )

    def add_base(
        self,
        ticker: str,
        data: pd.DataFrame,
    ):
        """
        `Base add function`

        - Main Base is assumed to be added first.
        - Main H_Base is assumed to be added last.
        - Other bases are the remaining ones. 
        """

        base = Base(
            ticker=ticker,
            data=data,
            index=self.index,
        )

        self.__bases.update(
            {ticker: base},
        )

        if ticker.upper() in _DEFAULT_PAIRS:
            self.__broker.add_curr(base)
        if ticker.upper() == _DEFAULT_CARRY:
            self.__broker.add_carry(base)

    def add_asset(
        self,
        ticker: str,
        data: pd.DataFrame,
        **commkwargs: Union[str, Number],
    ):
        asset = Asset(
            data=data,
            ticker=ticker,
            index=self.index,
        )

        if commkwargs:
            asset.config(
                **commkwargs,
            )

        self.__assets.update(
            {ticker: asset},
        )

    def add_hedge(
        self,
        ticker: str,
        hmethod: str,
        data: pd.DataFrame,
        **commkwargs: Union[str, Number],
    ):
        hedge = Hedge(
            data=data,
            ticker=ticker,
            hmethod=hmethod,
            index=self.index,
        )

        if commkwargs:
            hedge.config(
                **commkwargs,
            )

        self.__hedges.update(
            {ticker: hedge},
        )

    def run(self) -> Optional[pd.DataFrame]:
        if not self.__assets:
            return

        self.__pipeline.init()
        self.__strategy.init()

        for self.dt in self.loop:
            for data in self.datas.values(): 
                data.next()
            self.universe = self.__pipeline.run()
            
            self.__broker.beg_of_period()
            self.__strategy.next()
            self.__broker.end_of_period()

    def __len__(self) -> int:
        return len(self.__index)

    @property
    def index(self) -> Sequence[date]:
        return self.__index

    @property
    def loop(self) -> Sequence[date]:
        return self.__index[_DEFAULT_BUFFER+1:]

    @property
    def strategy(self) -> Strategy:
        return self.__strategy

    @property
    def base(self) -> Base:
        return tuple(self.__bases.values())[0]
    
    @property
    def h_base(self) -> Base:
        return tuple(self.__bases.values())[-1]

    @property
    def bases(self) -> Dict[str, Base]:
        return self.__bases

    @property
    def datas(self) -> Dict[str, Union[Base, Asset, Hedge]]:
        return {**self.__bases, **self.__assets, **self.__hedges}
