#! /usr/bin/env python3

import pandas as pd

from datetime import date, datetime
from typing import Dict, Sequence, Union

from .broker import Broker
from .strategy import Strategy

from .utils.bases import Base, Asset, Hedge
from .utils.config import _CURR, _PAIRS
from .utils.config import _DEFAULT_CASH, _DEFAULT_CURRENCY
from .utils.config import _DEFAULT_SDATE, _DEFAULT_EDATE


class Engine:

    """

    This object is responsible for orchestrating all
    other objects (Strategy, Broker, Position, ...)
    in order to properly run the simulation.

    Lots of features such as intraday operations,
    multi-calendar runs, live trading, etc. are
    still pending development.

    If cached is True, it will try to build a
    local cache of serialized data from the
    database.

    Database configuration is expected to be
    given in a .env file at the root of the
    program. The database specifications
    must comply with what is dictated by
    .env

    """

    def __init__(
        self,
        strategy: Strategy,
    ):

        self.__bases = {
            "base": None,
            "hbase": None,
        }

        self.__baselike = False
        self.__hedgelike = False

        self.__assets = {}
        self.__hedges = {}
        self.__currs = {}
        self.__carry = {}
        self.__holidays = []

        self.__sdate = _DEFAULT_SDATE
        self.__edate = _DEFAULT_EDATE
        self.__curr = _DEFAULT_CURRENCY
        self.__cash = _DEFAULT_CASH

        self.__broker: Broker = Broker(
            cash=self.__cash,
            curr=self.__curr,
        )

        self.__strategy: Strategy = strategy(
            broker=self.__broker,
        )

        self.__args_validation()

        self.__index: Sequence[date] = tuple(
            pd.bdate_range(
                start=self.__sdate,
                end=self.__edate,
                holidays=self.__holidays,
            ).date
        )

        pass

    def run(self) -> pd.DataFrame:
        self.__pre_run()

        for idx in self.index:
            self.__broker.next()
            self.__strategy.next()

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

        if ticker.upper() in _PAIRS:
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
                self.__baselike = True
            else:
                self.__bases.update(
                    {"hbase": base},
                )
                self.__hedgelike = True

    def add_asset(
        self,
        ticker: str,
        data: pd.DataFrame,
        **commkwargs: dict,
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
        **commkwargs: dict,
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

        self.__hedgelike = True

    def add_meta(
        self,
        asset: str,
        data: pd.DataFrame,
    ):
        pass

    def __build_pipeline(self):
        pass

    def __pre_run(self):
        if not self.__assets:
            msg = "No Data was provided!!"
            raise ValueError(msg)

        self.__build_pipeline()

        self.__strategy.datas = self.datas

        if self.__baselike:
            self.__strategy.config(
                self.__bases["base"],
            )

        else:
            for data in self.datas.values():
                self.__strategy.config(data)

    def __args_validation(self):

        if not (isinstance(self.__strategy, Strategy)):
            msg = "Arg `strategy` must be a `Strategy`"
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
            msg = "Sequence `holidays` must have date elements"
            raise TypeError(msg)

    @property
    def index(self) -> Sequence[date]:
        return self.__index

    @property
    def dt(self) -> str:
        return self.__index[0].isoformat()

    @property
    def datas(self) -> Dict[str, Union[Base, Asset]]:
        datas = {**self.__bases, **self.__assets}
        datas = {k: v for k, v in datas.items() if v is not None}
        return datas

    @property
    def strategy(self) -> Strategy:
        return self.__strategy
