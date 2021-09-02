#! /usr/bin/env python3

import pandas as pd
from numbers import Number
from uuid import uuid3, NAMESPACE_DNS
from collections import OrderedDict
from datetime import date, datetime
from typing import (
    Dict,
    List,
    Sequence,
    Union,
    Optional,
    Any,
)

from .broker import Broker
from .pipeline import Pipeline
from .strategy import Strategy
from .calendar import Calendar

from .utils.bases import Line, Base, Asset, Hedge
from .utils.math import fill_OHLC

from .utils.config import (
    _DEFAULT_BUFFER,
    _DEFAULT_VOLATILITY,
    _DEFAULT_CARRY,
    _DEFAULT_PAIRS,
    _DEFAULT_ECHO,
)


class Backtest:

    """
    `Backtest Class`

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
        **properties: str,
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
        self.__firstdate: date = self.__index[0]
        self.__lastdate: date = self.__index[-1]

        self.__factor: str = properties.get("factor")
        self.__market: str = properties.get("market")
        self.__asset: str = properties.get("asset")
        self.__vertices: List[int] = properties.get("vertices")

        self.__main: Line = Line(self.__index)
        self.__bases: Dict[str, Base] = OrderedDict()
        self.__assets: Dict[str, Asset] = OrderedDict()
        self.__hedges: Dict[str, Hedge] = OrderedDict()

        self.__broker: Broker = Broker(
            index=self.__index,
            echo=_DEFAULT_ECHO,
        )

        self.__pipeline: Pipeline = pipeline(
            main=self.__main,
            broker=self.__broker,
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
            data=fill_OHLC(data),
            ticker=ticker,
            index=self.index,
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
            data=fill_OHLC(data),
            ticker=ticker,
            hmethod=hmethod,
            index=self.index,
            **commkwargs,
        )

        self.__hedges.update(
            {ticker: hedge},
        )

    def run(self) -> Dict[str, pd.DataFrame]:
        if not self.__assets:
            return

        self.__pipeline.init()
        self.__strategy.init()
        self.config_backtest()

        while self.dt < self.__lastdate:
            self.__advance()
            self.__broker.beg_of_period()
            self.__pipeline.next()
            self.__strategy.next()
            self.__broker.end_of_period()

        return {
            "meta": self.__properties,
            "quotas": self.__broker.df,
            "records": self.__broker.rec,
            "broker": self.__broker,
        }

    def __advance(self):
        self.__main.next()
        self.__broker.next()
        for data in self.datas.values():
            data.next()

    def config_backtest(self):
        self.__hash = {
            "factor": self.__factor,
            "market": self.__market,
            "asset": self.__asset,
            "vertices": self.__vertices,
            "model": self.__strategy.__class__.__name__,
            "params": dict(self.__strategy.get_params()),
        }

        self.__uid = uuid3(
            NAMESPACE_DNS,
            str(self.__hash),
        )

        self.__properties = pd.DataFrame.from_records(
            [
                {
                    **self.__hash,
                    "uid": self.__uid.hex,
                    "sdate": self.__firstdate.isoformat(),
                    "edate": self.__lastdate.isoformat(),
                    "updtime": datetime.now().isoformat(),
                    "budget": _DEFAULT_VOLATILITY,
                    "buffer": _DEFAULT_BUFFER,
                    "bookname": self.bookname,
                }
            ]
        )

    @property
    def dt(self) -> date:
        return self.__main[0]

    @property
    def bookname(self) -> str:
        return f"{self.__factor}-{self.__market}-{self.__asset}"

    @property
    def index(self) -> Sequence[date]:
        return self.__index

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
