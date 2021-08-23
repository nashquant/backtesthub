#! /usr/bin/env python3

from backtesthub.pipeline import Pipeline
import numpy as np
from numbers import Number
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
        pipeline: Pipeline,
        bases: Dict[str, Base],
        assets: Dict[str, Asset],
        hedges: Dict[str, Hedge],
    ):

        self.__broker = broker
        self.__pipeline = pipeline
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
        *args: Number,
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

        current = self.get_current(data)
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
            if target is None:
                msg="Need to assign a Target"
                raise ValueError(msg)

            size = target * equity / price

        elif method == _METHOD["SIZE"]:
            if target is None:
                msg="Need to assign a Target"
                raise ValueError(msg)
            
            size = target

        else:
            msg = "Method still not Implemented"
            raise NotImplementedError(msg)

        if size==current:
            return
        else:
            delta = size - current

        has_position = (not current == 0)
        has_tresh = (thresh > 0) 

        if has_position and has_tresh:
            stimulus = abs(delta) / abs(current)
            if stimulus < thresh:
                return

        self.__broker.new_order(
            data=data,
            size=delta,
            limit=price,
        )

    def get_universe(self) -> Sequence[Union[Asset, Hedge]]:
        return self.__pipeline.universe

    def get_current(self, data: Union[Asset, Hedge]) -> Number:

        if type(data) not in (Asset, Hedge):
            msg = "Data must be of type Asset/Hedge"
            raise TypeError(msg)

        position = self.__broker.get_position(data.ticker)
        size = position.size if position is not None else 0

        return size

    @property
    def base(self) -> Base:
        return tuple(self.__bases.values())[0]

    @property
    def bases(self) -> Dict[str, Base]:
        return self.__bases

    @property
    def assets(self) -> Dict[str, Asset]:
        return self.__assets

    @property
    def hedges(self) -> Dict[str, Hedge]:
        return self.__hedges

    @property
    def pipeline(self) -> Pipeline:
        return self.__pipeline