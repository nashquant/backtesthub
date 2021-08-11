#! /usr/bin/env python3

import numpy as np
import pandas as pd

from numbers import Number
from datetime import date, datetime
from typing import List, Dict, Union, Any

from .broker import Broker
from .strategy import Strategy
from .utils.types import Asset
from .utils.config import _SCHEMA



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
        sdate: Union[date, datetime],
        edate: Union[date, datetime],
        cash: float = float("10e6"),
    ):
        
        self.__strategy = strategy
        self.__sdate = sdate
        self.__edate = edate
        self.__cash = cash
        
        self.__datas = {}
        self.__holidays = []

        if not (issubclass(self.__strategy, Strategy)):
            msg = "Arg `strategy` must be a Strategy sub-type"
            raise TypeError(msg)

        self.__broker = Broker(
            datas=self.__datas,
            cash=self.__cash,
        )

    def addData(
        self, 
        ticker: str, 
        data: pd.DataFrame, 
        **comminfo: Dict[str, Any]
    ):
        
        asset = Asset(
            ticker = ticker,
            data = data
        )

        ### << Check comminfo kwargs validity >> ##

        asset.config(**comminfo)

    def __log(self, txt: str):

        msg = f"({self.dt.isoformat()}), {txt}"

        print(msg)

    def __set_buffer(self):

        pass

    def run(self) -> Dict[str, Number]:

        if not self.__datas:
            msg = "No Data was inputed"
            raise ValueError(msg)

        broker = self.__broker
        strategy = self.__strategy

        strategy.init()

        ## <<<< Implement buffer handling! >>>> ##

        self.__set_buffer()

        ## <<<< Implement "logging lib" error tracing! >>>> ##
        ## <<<< Implement index by sdate, edate, holidays >>>> ##

        while True:

            for ticker in self.__datas:

                data = self.__datas[ticker]
                data._set_buffer(self.__buffer)

                broker.next()
                strategy.next()

                self.__buffer += 1

    
    @property
    def dt(self):
        pass
        