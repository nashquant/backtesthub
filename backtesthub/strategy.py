#! /usr/bin/env python3

from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, Union, Any

from .broker import Broker
from .pipeline import Pipeline
from .utils.bases import *
from .utils.config import _MODE
from .utils.checks import derive_params


class Strategy(metaclass=ABCMeta):
    def __init__(
        self,
        broker: Broker,
        bases: Dict[str, Optional[Base]],
        assets: Dict[str, Optional[Asset]],
        hedges: Dict[str, Optional[Hedge]],
    ):

        self.__broker = broker
        self.__bases = bases
        self.__assets = assets
        self.__hedges = hedges
        
        self.__indicators = {}
        self.__mode = _MODE["V"]

    @abstractmethod
    def init():
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
        *args: Union[str, int, float],
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
            msg = f"`Mode` {self.__mode} not implemented"
            raise NotImplementedError(msg)

        if not len(data) == len(ind):
            msg = f"{name}: error in Line length"
            raise ValueError(msg)

        key = f"{ticker} {name}"
        self.__indicators.update({key: ind})

        return ind

    def buy(self, data: Union[Asset, Hedge], size: float, price: Optional[float]):
        self.__broker.order(data=data, size=abs(size), limit=price)

    def sell(self, data: Union[Asset, Hedge], size: float, price: Optional[float]):
        self.__broker.order(data=data, size=-abs(size), limit=price)

    @property
    def indicators(self) -> Dict[str, Line]:
        return self.__indicators

    @property
    def bases(self) -> Dict[str, Base]:
        return self.__bases

    @property
    def assets(self) -> Dict[str, Asset]:
        return self.__assets

    @property
    def hedges(self) -> Dict[str, Hedge]:
        return self.__hedges
