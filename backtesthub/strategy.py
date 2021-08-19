#! /usr/bin/env python3

import numpy as np

from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, Union, Optional, Sequence

from .broker import Broker

from .indicators import Default
from .utils.math import EWMAVolatility
from .utils.bases import Line, Base, Asset, Hedge
from .utils.config import (
    _DEFAULT_VOLATILITY,
    _DEFAULT_THRESH,
    _METHOD,
)


class Strategy(metaclass=ABCMeta):
    def __init__(
        self,
        broker: Broker,
        bases: Dict[str, Base],
        assets: Dict[str, Asset],
        hedges: Dict[str, Hedge],
    ):

        self.__broker = broker
        self.__bases = bases
        self.__assets = assets
        self.__hedges = hedges

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
        data: Union[Base, Asset, Hedge],
        func: Callable = Default,
        *args: Union[str, int, float],
    ):

        """
        Declare indicator.

        """

        try:
            ind = func(data, *args)

        except Exception as e:
            raise Exception(e)

        if not len(data) == len(ind):
            msg = f"Line length not compatible"
            raise ValueError(msg)

        indicator = Line(array=ind)
        signal = Line(array=np.sign(ind))

        data.add_line(
            name="signal",
            line=signal,
        )

        data.add_line(
            name="indicator",
            line=indicator,
        )

        if "volatility" not in set(data.lines):
            self.V(data=data)

    def V(
        self,
        data: Union[Base, Asset, Hedge],
        func: Callable = EWMAVolatility,
        *args: Union[str, int, float],
    ):

        try:
            vol = func(data, *args)

        except Exception as e:
            raise Exception(e)

        data.add_line(
            name="volatility",
            line=Line(array=vol),
        )

    def broadcast(
        self,
        base: Base,
        assets: Dict[str, Asset],
        lines: Sequence[str] = [],
    ):

        schema = set(base.schema)
        if not lines: 
            lines = ["signal", "volatility"]
            
        for asset in assets.values():
            for line in lines:
                line = line.lower()

                if not line in schema:
                    continue

                asset.add_line(
                    name=line,
                    line=eval(f"base.{line}"),
                )

    def order(
        self,
        data: Union[Asset, Hedge],
        target: Optional[float] = None,
        price: Optional[float] = None,
        thresh: float = _DEFAULT_THRESH,
        method: str = "EWMA",
    ):

        current = self.get_current()
        equity = self.__broker[0]
        method = _METHOD[method]

        price = data.close[0] * data.multiplier

        if method == _METHOD["EWMA"]:
            
            signal = data.signal[0]

            vol_target = _DEFAULT_VOLATILITY
            vol_asset = data.volatility[0]
            target = vol_asset / vol_target

            size = signal * target * equity / price

        elif method == _METHOD["EXPO"]:
            assert target is not None

            size = target * equity / price

        elif method == _METHOD["SIZE"]:
            assert target is not None
            size = target

        else:
            msg = "Method still not Implemented"
            raise NotImplementedError(msg)

        if size==current:
            return
        else:
            delta = size - current

        if current != 0 and thresh > 0:
            stimulus = abs(delta) / abs(current)
            if stimulus < thresh:
                return

        self.__broker.new_order(
            data=data,
            size=delta,
            limit=price,
        )

    def get_current(self, data: Union[Asset, Hedge]):

        if type(data) not in (Asset, Hedge):
            msg = "Data must be of type Asset/Hedge"
            raise TypeError(msg)

        position = self.__broker.get_position(data.ticker)
        size = position.size if position is not None else 0

        return size

    @property
    def indicators(self) -> Dict[str, str]:
        return self.__indicators

    @property
    def base(self) -> Optional[Base]:
        return self.__bases.get("base")

    @property
    def bases(self) -> Dict[str, Base]:
        return self.__bases

    @property
    def assets(self) -> Dict[str, Asset]:
        return self.__assets

    @property
    def hedges(self) -> Dict[str, Hedge]:
        return self.__hedges
