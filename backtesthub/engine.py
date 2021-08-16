#! /usr/bin/env python3

from backtesthub.pipeline import Pipeline
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

    Some settings may be changed through environment
    variables configuration. Refer to .utils.config
    to get more info.

    """

    def __init__(
        self,
        strategy: Strategy,
    ):

        self.__bases = {
            "base": None,
            "hbase": None,
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
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
        )

        self.__args_validation()

        self.__index: Sequence[date] = tuple(
            pd.bdate_range(
                start=self.__sdate,
                end=self.__edate,
                holidays=self.__holidays,
            ).date
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

            else:
                self.__bases.update(
                    {"hbase": base},
                )

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
        **commkwargs: dict,
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

    def __pre_run(self):
        if not self.__assets:
            msg = "No Data was provided!!"
            raise ValueError(msg)

        self.__pipeline = Pipeline(
            bases=self.__bases,
            assets=self.__assets,
            hedges=self.__hedges,
            case=self.__case,
        )

        self.__strategy.init()

    def run(self) -> pd.DataFrame:
        self.__pre_run()

        for self.dt in self.__index:
            self.__next()

    def __next(self):

        self.__broker.next()
        self.__strategy.next()

        for data in self.all_datas.values():
            data._Data__forward()

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

    def __len__(self) -> int:
        return len(self.__index)

    @property
    def index(self) -> Sequence[date]:
        return self.__index

    @property
    def strategy(self) -> Strategy:
        return self.__strategy

    @property
    def pipeline(self) -> Pipeline:
        return self.__pipeline

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
