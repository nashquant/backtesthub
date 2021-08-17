#! /usr/bin/env python3

import numpy as np

from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, Sequence, Union, Optional

from .broker import Broker
from .utils.config import _MODE
from .utils.checks import derive_params
from .utils.bases import Line, Base, Asset, Hedge


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

        self.__mode: str = _MODE["V"]
        self.__indicators: Dict[str, str] = {}

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
                ind = func(data, *args)

            except Exception as e:
                raise Exception(e)

        else:
            msg = f"`Mode` {self.__mode} not implemented"
            raise NotImplementedError(msg)

        if not len(data) == len(ind):
            msg = f"{name}: error in Line length"
            raise ValueError(msg)

        strength = Line(array=ind)
        signal = Line(array=np.sign(ind))

        self.__indicators.update(
            {ticker: name},
        )

        data.add_line(
            name="signal",
            line=signal,
        )

        data.add_line(
            name="strength",
            line=strength,
        )

    def broadcast(
        self, 
        base: Optional[Base] = None,
        assets: Optional[Sequence[Asset]] = [],
    ):

        if not base: base = self.bases["base"]
        if not assets: assets = self.assets.values()

        for asset in self.assets:
            asset.add_line(
                name="signal",
                line=base.signal,
            )

            asset.add_line(
                name="strength",
                line=base.strength,
            )

    def buy(self, data: Union[Asset], size: float, price: Optional[float]):
        self.__broker.order(data=data, size=abs(size), limit=price)

    def sell(self, data: Union[Asset], size: float, price: Optional[float]):
        self.__broker.order(data=data, size=-abs(size), limit=price)

    @property
    def indicators(self) -> Dict[str, str]:
        return self.__indicators

    @property
    def bases(self) -> Dict[str, Base]:
        return {k: v for k, v in self.__bases.items() if v is not None}

    @property
    def assets(self) -> Dict[str, Asset]:
        return {k: v for k, v in self.__assets.items() if v is not None}

    @property
    def hedges(self) -> Dict[str, Hedge]:
        return {k: v for k, v in self.__hedges.items() if v is not None}
