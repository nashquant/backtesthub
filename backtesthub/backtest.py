#! /usr/bin/env python3

import numpy as np
import pandas as pd

from numbers import Number
from warnings import filterwarnings
from datetime import date, datetime
from typing import List, Dict, Union, Sequence

from .broker import Broker
from .strategy import Strategy
from .utils.config import _SCHEMA
from .utils.types import Asset, Line


filterwarnings("ignore")


class Backtest:

    """
    __INTRO__

    * Backtest Initialization. This object is responsible for
        orchestrating all complementary objects (Strategy, Broker, 
        Position, ...) in order to properly run the simulation. 
        Lots of features such as intraday operations, multi-calendar 
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
        datas: Dict[str, pd.DataFrame],
        sdate: Union[date, datetime],
        edate: Union[date, datetime],
        holidays: Sequence[date],
        cash: float = float("10e6"),
    ):

        ## <<SET VARIABLES>> ##

        self.__datas = datas
        self.__strategy = strategy
        self.__holidays = holidays
        self.__sdate = sdate
        self.__edate = edate
        self.__cash = cash

        self.__ohlc: List[str] = _SCHEMA["OHLC"]
        self.__ohlcv: List[str] = _SCHEMA["OHLCV"]

        ## <<VERIFY INTEGRITY>> ##

        if not (issubclass(self.__strategy, Strategy)):
            msg = "Arg `strategy` must be a Strategy sub-type"
            raise TypeError(msg)

        for data in self.__datas:
            if not isinstance(data, pd.DataFrame):
                msg = "Arg `data` must be a pandas.DataFrame"
                raise TypeError()

            if len(data) == 0:
                msg = "OHLC `data` is empty"
                raise ValueError()

            rename = {c: c.lower() for c in data.columns}
            data.rename(columns=rename, inplace=True)

            if len(data.columns.intersection(set(self.__ohlc))) < 4:
                msg = "Arg `datas` must hold OHLC schemed pandas.DataFrame"
                raise ValueError(msg)

        ## <<BUILD ENTITIES>> ##

        self.__broker = Broker(
            datas=self.__datas,
            cash=self.__cash,
        )

    def run(self) -> Dict[str, Number]:

        broker = self.__broker
        strategy = self.__strategy

        strategy.init()

        ## <<<< Implement buffer handling! >>>> ##

        buffer = 200

        ## <<<< Implement "logging lib" error tracing! >>>> ##
        ## <<<< Implement index by sdate, edate, holidays >>>> ##

        while True:

            for ticker in self.__datas:

                data = self.__datas[ticker]
                data._set_buffer(buffer)

                broker.next()
                strategy.next()

                buffer += 1