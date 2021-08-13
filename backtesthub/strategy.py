#! /usr/bin/env python3

import pandas as pd
from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, Union

from .order import Order
from .broker import Broker
from .utils.bases import *
from .utils.config import _MODE

class Strategy(metaclass=ABCMeta):

    def __init__(self):

        if not hasattr(self, "mode"):
            self.mode = "V"

        if self.mode not in _MODE:
            msg = "Calculation Mode not implemented"
            raise NotImplementedError(msg)
        else:
            self.__mode=_MODE[self.mode]
        
        self.__datas={}
        self.__indicators={}

    def addData(
        self, 
        data: Union[Base, Asset],
    ):
        pass

    def addBroker(
        self,
        broker: Broker
    ):
        self.__broker = broker

    @abstractmethod
    def init(self):
        """
        * To initialize the strategy, override this method.

        * Declare indicators (with `backtesting.backtesting.Strategy.I`).

        * Precompute what needs to be precomputed or can be precomputed
          in a vectorized fashion before the strategy starts.

        * If you extend composable strategies from `backtesting.lib`,

        * make sure to call `super().init()`
        """

    @abstractmethod
    def next(self):
        """
        * Main strategy runtime method, called as each new
          `backtesting.backtesting.Strategy.data` instance
          (row; full candlestick bar) becomes available.

        * This is the main method where strategy decisions
          upon data precomputed in `backtesting.backtesting.
          Strategy.init` take place.

        * If you extend composable strategies from `backtesting.lib`,

        * make sure to call `super().next()`!
        """

    def I(
        self,
        f: Callable,
        **params: Dict,
    ):

        """
        Declare indicator.

        * Inspired by Backtesting.py project:
        https://github.com/kernc/backtesting.py.git

        * An indicator is just a line of values,but one that is revealed
          gradually in `backtesting.backtesting.Strategy.next` much like
          `backtesting.backtesting.Strategy.data` is.

        * `func` is a function that returns the indicator array(s) of
          same length as `backtesting.backtesting.Strategy.data`.

        * For example, using simple moving average function from TA-Lib:
            def init():
                self.sma = self.I(ta.SMA, self.data.Close, p1 = self.p1, p2 = self.p2)
        """

        name = params.pop("name", None)

        if name is None:
            name = f"{f.__name__}{tuple(params.items())}"

        if self.__mode == _MODE["V"]:
            try:
                ind = f(**params)
            except Exception as e:
                raise Exception(e)

            if isinstance(ind, pd.DataFrame):
                ind = ind.values.T

            self.__indicators.update({
                name: Line(array = ind)
            })

    def buy(self, ticker: str, size: float, price: float):
        return self.__broker.order(ticker, abs(size), price)

    def sell(self, ticker: str, size: float, price: float):
        return self.__broker.order(ticker, -abs(size), price)

    @property
    def indicators(self) -> Dict[str, Line]:
        return self.__indicators
