#! /usr/bin/env python3

import pandas as pd
from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, Union

from .order import Order
from .broker import Broker
from .utils.bases import *
from .utils.config import _MODE
from .utils.checks import derive_params


class Strategy(metaclass=ABCMeta):
    def __init__(
        self,
        broker: Broker,
        mode: str = _MODE["V"],
    ):

        self.__broker = broker
        self.__indicators = {}
        self.__datas = {}
        self.__mode = mode

    @abstractmethod
    def config(data: Union[Base, Asset]):
        """
        * To configure the strategy, override this method.

        * Declare indicators (with `backtesting.backtesting.Strategy.I`).

        * Precompute what needs to be precomputed or can be precomputed
          in a vectorized fashion before the strategy starts.

        """

    @abstractmethod
    def next():
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
        func: Callable,
        data: Union[Base, Asset],
        *args,
    ):

        """
        Declare indicator.

        * Inspired by Backtesting.py project:
        https://github.com/kernc/backtesting.py.git

        """

        ticker = data.ticker
        params = derive_params(args)
        name = f"{func.__name__}({params})"

        if self.__mode == _MODE["V"]:

            try:
                ind = Line(func(data, *args))
            except Exception as e:
                raise Exception(e)

        else:
            msg = f"`Mode` {self.__mode} not available"
            raise NotImplementedError(msg)

        if not len(data) == len(ind):
            msg = f"{name}"

        key = f"{ticker} {name}"
        self.__indicators.update({key: ind})

        return ind

    def buy(self, data: Union[Asset, Hedge], size: float, price: Optional[float]):
        return self.__broker.order(data, abs(size), price)

    def sell(self, data: Union[Asset, Hedge], size: float, price: Optional[float]):
        return self.__broker.order(data, -abs(size), price)

    def get_data(self, ticker: str) -> Optional[Union[Base, Asset]]:
        if ticker not in self.__datas:
            return
        return self.__datas[ticker]

    @property
    def datas(self) -> Dict[str, Union[Base, Asset]]:
        return self.__datas

    @datas.setter
    def datas(
        self,
        datas: Dict[str, Union[Base, Asset]],
    ):
        """
        Setter allows only one assignment
        """

        if self.__datas:
            return
        self.__datas = datas

    @property
    def indicators(self) -> Dict[str, Line]:
        return self.__indicators
