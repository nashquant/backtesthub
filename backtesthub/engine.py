#! /usr/bin/env python3

import pandas as pd

from numbers import Number
from datetime import date, datetime
from typing import List, Dict, Sequence, Any

from .broker import Broker
from .strategy import Strategy
from .pipeline import Pipeline
from .utils.types import Line, Base, Asset, Hedge
from .utils.config import _SCHEMA, _CASH, _CURR


class Engine:

    """
    __INTRO__

    * This object is responsible for orchestrating all
      complementary objects (Strategy, Broker, Position, ...)
      in order to properly run the simulation.

    * Lots of features such as intraday operations, multi-calendar
      runs, live trading, etc. are still pending development.

    __KWARGS__

    * `datas` is a sequence of `pd.DataFrame` with columns:
        `Open`, `High`, `Low`, `Close`, (optionally) `Volume`.

        The passed data frame can contain additional columns that
        can be used by the strategy.

        DataFrame index can be either a date/datetime index.
        Still not implemented other formats (str isoformat, int,...)

    * `strategy` is a `backtesting.backtesting.Strategy`
        __subclass__ (NOT AN INSTANCE!!).

    * Global index is derived by the combination of:

        - `sdate` the start date for the simulation.
        - `edate` the end date for the simulation.
        - `holidays` a list of non-tradable days.

    * `cash` is the initial cash to start with.

    """

    def __init__(
        self,
        strategy: Strategy,
        sdate: date,
        edate: date,
        cash: float = _CASH,
        multi: bool = False,
        curr: str = "BRL",
    ):
        self.__strategy = strategy
        self.__sdate = sdate
        self.__edate = edate
        self.__cash = cash
        self.__multi = multi
        self.__curr = curr

        self.__bases = {"A": None, "H": None}

        self.__assets = {}
        self.__hedges = {}
        self.__currs = {}
        self.__holidays = []

        self.__type_check()

        self.__broker = Broker(
            cash=self.__cash,
            curr=self.__curr,
        )

        self.__index = Line(
            array = pd.bdate_range(
                start=self.__sdate,
                end=self.__edate,
                holidays=self.__holidays,
            )
        )

    def addBase(self, ticker: str, data: pd.DataFrame, hedge: bool = False):
        """
        Only one base allowed per side.
        """

        if not hedge:
            asset = Base(
                data=data,
                ticker=ticker,
            )

            self.__bases.update({"A": asset})

        else:
            hedge = Base(
                data=data,
                ticker=ticker,
            )

            self.__bases.update({"H": hedge})

    def addCurrency(
        self,
        curr_base: str,
        curr_target: str,
        data: pd.DataFrame,
    ):
        """
        Example:
        curr_base = "USD"
        curr_target = "BRL"
        curr = "USDBRL"
        """

        if curr_base == curr_target:
            return
        if curr_base not in _CURR:
            return
        if curr_target not in _CURR:
            return

        curr = f"{curr_base}{curr_target}"
        base = Base(
            ticker=curr,
            data=data,
        )

        self.__currs.update({curr: base})

    def addAsset(
        self,
        ticker: str,
        data: pd.DataFrame,
        meta: pd.DataFrame = None,
        **comminfo: Dict[str, Any],
    ):
        asset = Asset(
            data=data,
            ticker=ticker,
        )

        asset.config(**comminfo)

        if meta is not None:
            pass

        self.__assets.update({ticker: asset})

    def addHedge(
        self,
        ticker: str,
        hmethod: str,
        data: pd.DataFrame,
        meta: pd.DataFrame = None,
        **comminfo: Dict[str, Any],
    ):
        hedge = Hedge(
            data=data,
            ticker=ticker,
            hmethod=hmethod,
        )

        hedge.config(**comminfo)

        if meta is not None:
            pass

        self.__hedges.update({ticker: hedge})

    def init(self):

        self.__build_pipeline()
        self.__pipeline.init()

        self.__strategy.init()

    def run(self) -> Dict[str, Number]:

        if not {**self.__bases, **self.__assets}:
            msg = "No Data was provided!!"
            raise ValueError(msg)

        for idx in self.index:

            universe = self.__pipeline.run()

            self.__broker.next()
            self.__strategy.next()

    def __build_pipeline(self):

        if self.__multiasset:

            self.__pipeline = Pipeline(assets=self.__assets)

    def __type_check(self):

        if not (issubclass(self.__strategy, Strategy)):
            msg = "Arg `strategy` must be a Strategy sub-type"
            raise TypeError(msg)

        if not self.__curr in _CURR:
            msg = "Unknown `currency` requested"
            raise ValueError(msg)

        if isinstance(self.__sdate, datetime):
            self.__sdate = self.__sdate.date()

        if not isinstance(self.__sdate, date):
            msg = "Arg `sdate` must be a date"
            raise TypeError(msg)

        if isinstance(self.__edate, datetime):
            self.__edate = self.__edate.date()

        if not isinstance(self.__edate, date):
            msg = "Arg `edate` must be a date"
            raise TypeError(msg)

        if not isinstance(self.__holidays, Sequence):
            msg = "Arg `holidays` must be a Sequence"
            raise TypeError(msg)

        if not all(isinstance(dt, date) for dt in self.__holidays):
            msg = "Sequence `holidays` must have date scalars"
            raise TypeError(msg)

    @property
    def __datas(self):
        return {**self.__assets, **self.__hedges}

    @property
    def index(self):
        return self.__index

    @property
    def dt(self):
        return self.__index[0]
