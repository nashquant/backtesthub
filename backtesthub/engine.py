#! /usr/bin/env python3

from backtesthub.pipeline import Pipeline
import pandas as pd

from datetime import date
from numbers import Number
from typing import Dict, Sequence, Union

from .broker import Broker
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
        calendar: Calendar,
    ):

        if not (issubclass(strategy, Strategy)):
            msg = "Arg `strategy` must be a `Strategy` subclass!"
            raise TypeError(msg)

        if not (isinstance(calendar, Calendar)):
            msg = "Arg `calendar` must be a `Calendar` instance!"
            raise TypeError(msg)

        self.__calendar = calendar
        self.__index = calendar.index

        self.__bases = {
            "base": None,
            "h_base": None,
        }

        self.__case = dict(
            stocklike=None,
            rateslike=None,
            multiasset=None,
            h_stocklike=None,
            h_rateslike=None,
            h_multiasset=None,
        )

        self.__assets = {}
        self.__hedges = {}
        self.__currs = {}
        self.__carry = {}
        self.__universe = []

        self.__broker: Broker = Broker(
            index=self.__index,
        )

        self.__strategy: Strategy = strategy(
            broker=self.__broker,
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
        )

        self.__pipeline: Pipeline = Pipeline(
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
            case=self.__case,
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

        if ticker.upper() in _DEFAULT_PAIRS:
            self.__currs.update(
                {ticker: base},
            )
        if ticker.upper() == "CARRY":
            self.__carry.update(
                {ticker: base},
            )

        if main:
            if not hedge:
                self.__bases.update(
                    {"base": base},
                )

            else:
                self.__bases.update(
                    {"hbase": base},
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

        if self.__case["stocklike"] is None:
            self.__case["stocklike"] = asset.stocklike
            self.__case["rateslike"] = asset.rateslike
            self.__case["multiasset"] = False
        elif (
            not self.__case["stocklike"] == asset.stocklike
            and not self.__case["rateslike"] == asset.rateslike
        ):
            msg = "Case not acknowledged as valid"
            raise ValueError(msg)
        else:
            self.__case["multiasset"] = True

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

        if self.__case["h_stocklike"] is None:
            self.__case["h_stocklike"] = hedge.stocklike
            self.__case["h_rateslike"] = hedge.rateslike
            self.__case["h_multiasset"] = False
        elif (
            not self.__case["h_stocklike"] == hedge.stocklike
            and not self.__case["h_rateslike"] == hedge.rateslike
        ):
            msg = "Case not acknowledged as valid"
            raise ValueError(msg)
        else:
            self.__case["multiasset"] = True

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

            self.__broker.next()
            self.__strategy.next()

            new = self.__pipeline.run(
                date=self.dt,
                old=self.__universe,
            )

            self.__universe = new

            for data in self.__universe:
                data.next()

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
