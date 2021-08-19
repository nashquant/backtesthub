#! /usr/bin/env python3

import pandas as pd

from datetime import date
from numbers import Number
from collections import OrderedDict
from typing import Dict, Sequence, Union

from .broker import Broker
from .pipeline import Pipeline
from .strategy import Strategy
from .calendar import Calendar

from .utils.bases import Base, Asset, Hedge

from .utils.config import (
    _DEFAULT_PAIRS,
    _DEFAULT_BUFFER,
)


class Engine:

    """

    This object is responsible for orchestrating all
    other objects (Strategy, Broker, Position, ...)
    in order to properly run the simulation.

    Lots of features such as intraday operations,
    multi-calendar runs, live trading, etc. are
    still pending development.

    Some settings may be changed through environment
    variables configuration. Refer to .utils.config
    to get more info.

    """

    def __init__(
        self,
        strategy: Strategy,
        pipeline: Pipeline,
        calendar: Calendar,
    ):

        if not (issubclass(strategy, Strategy)):
            msg = "Arg `strategy` must be a `Strategy` subclass!"
            raise TypeError(msg)

        if not (issubclass(pipeline, Pipeline)):
            msg = "Arg `pipeline` must be a `Pipeline` subclass!"
            raise TypeError(msg)

        if not (isinstance(calendar, Calendar)):
            msg = "Arg `calendar` must be a `Calendar` instance!"
            raise TypeError(msg)

        self.__index: Sequence[date] = calendar.index
        self.__bases: Dict[str, Base] = OrderedDict()
        self.__assets: Dict[str, Asset] = OrderedDict()
        self.__hedges: Dict[str, Hedge] = OrderedDict()
        self.__obases: Dict[str, Base] = OrderedDict()
        self.__currs: Dict[str, Base] = OrderedDict()
        self.__carry: Dict[str, Base] = OrderedDict()
        self.__universe: Sequence[str] = list()

        self.__broker: Broker = Broker(
            index=self.__index,
        )

        self.__pipeline: Pipeline = pipeline(
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
        )

        self.__strategy: Strategy = strategy(
            broker=self.__broker,
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
        )

    def add_base(
        self,
        ticker: str,
        data: pd.DataFrame,
        hedge: bool = False,
        main: bool = True,
    ):
        """

        `Main` is a boolean that indicates
        whether the asset is the main base
        or not. You can have only one core
        base per side, but multiple non-core
        ones

        PS: Only one base allowed per side.
        "base": "Asset" Side
        "hbase": "Hedge" Side

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
            self.__currs.update(
                {ticker: base},
            )
        if ticker.upper() == "CARRY":
            self.__carry.update(
                {ticker: base},
            )

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

    def run(self) -> pd.DataFrame:

        if not self.__assets:
            return

        self.__pipeline.init()
        self.__strategy.init()

        for self.dt in self.loop:

            new = self.__pipeline.next(
                date=self.dt,
                old=self.__universe,
            )

            ## << Handle closing positions >> ##

            self.__universe = new

            for data in new:
                data.next()

            self.__broker.next()
            self.__strategy.next()

    def __len__(self) -> int:
        return len(self.__index)

    @property
    def index(self) -> Sequence[date]:
        return self.__index

    @property
    def loop(self) -> Sequence[date]:
        return self.__index[_DEFAULT_BUFFER:]

    @property
    def strategy(self) -> Strategy:
        return self.__strategy

    @property
    def base(self) -> Base:
        return self.__bases["base"]
    
    @property
    def h_base(self) -> Base:
        return self.__bases["h_base"]
    
    @property
    def obases(self) -> Dict[str, Base]:
        return self.__obases

    @property
    def universe(self) -> Sequence[Asset]:
        return self.__universe

    @property
    def datas(self) -> Dict[str, Union[Base, Asset]]:
        datas = {**self.__bases, **self.__assets}
        datas = {k: v for k, v in datas.items() if v is not None}
        return datas

    @property
    def all_datas(self) -> Dict[str, Union[Base, Asset, Hedge]]:
        datas = {**self.datas, **self.__hedges}
        datas = {k: v for k, v in datas.items() if v is not None}
        return datas
