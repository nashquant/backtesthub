#! /usr/bin/env python3

import math
import numpy as np
from numbers import Number
from abc import ABCMeta, abstractmethod
from collections import OrderedDict, defaultdict as ddict
from typing import Callable, Dict, Union, Optional, Sequence

from .broker import Broker
from .pipeline import Pipeline
from .utils.math import EWMAVolatility
from .utils.bases import Line, Base, Asset
from .utils.config import (
    _DEFAULT_VOLATILITY,
    _DEFAULT_MIN_SIZE,
    _DEFAULT_CURRENCY,
    _DEFAULT_SIZING,
    _DEFAULT_THRESH,
    _MIN_VOL,
    _METHOD,
)


class Strategy(metaclass=ABCMeta):

    """
    `Strategy Class`

    Explanation... 
    
    """

    def __init__(
        self,
        broker: Broker,
        pipeline: Pipeline,
        bases: Dict[str, Base],
        assets: Dict[str, Asset],
    ):

        self.__broker = broker
        self.__pipeline = pipeline
        self.__bases = bases
        self.__assets = assets
        self.__params = OrderedDict()

    @abstractmethod
    def init():
        """ """

    @abstractmethod
    def next():
        """ """

    def I(
        self,
        data: Union[Base, Asset],
        func: Callable,
        name: Optional[str] = None,
        **kwargs: Number,
    ):
        """
        `Indicator Assignment`

        - Takes a custom function, a data structre
          that can either be a Base or an Asset
          and some parameters to be passed to func.

        - The function is supposed to receive the
          data structure, and it is expected that
          the function can manipulate the data "schema".

        - The function should then perform calculations
          in a Line of the object and return an array-like
          object containing only numbers, that will then,
          be converted to indicators and signal lines
        """
        try:
            ind = func(data, *kwargs.values())
        except Exception as e:
            raise Exception(e)

        if kwargs:
            self.__params.update(kwargs)

        if not len(data) == len(ind):
            msg = f"Line length not compatible"
            raise ValueError(msg)

        signal = Line(array=ind)
        name = name or "signal"

        data.add_line(
            name=name,
            line=signal,
        )

    def V(
        self,
        data: Union[Base, Asset],
        func: Callable = EWMAVolatility,
        **kwargs: Union[str, int, float],
    ):
        """
        `Volatility Assignment`

        Basically does the same job as `self.I`
        but it is applied to volatility calcs.
        """
        try:
            vol = func(data, *kwargs.values())
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
        """
        `Lines Broadcasting`

        Very important object that allows one
        to assign the indicator/signal lines
        of an object to another one.

        Therefore, it allows one to calculate
        signals/volatility/indicators using a
        `Base` data, and transfer the results
        to one or multiple other `Assets` data.

        """
        default = ["signal", "volatility"]

        base_lines = list(base.lines)
        new_lines = lines or default

        for asset in assets.values():
            for line in new_lines:
                line = line.lower()
                if not line in base_lines:
                    continue
                obj = eval(f"base.{line}")

                asset.add_line(
                    name=line,
                    line=Line(obj),
                )

    def sizing(
        self,
        data: Asset,
        texpo: Optional[float] = None,
        method: str = _DEFAULT_SIZING,
        min_size: int = _DEFAULT_MIN_SIZE,
    ) -> Optional[Number]:

        """
        `Order Sizer`

        Very important method that allows one
        to compute with great flexibility the
        order size to be sent to the broker.

        First thing is to correctly select
        the method to be employed, which by
        "default" is the inverse volatility
        sizing.

        Obs: For other methods, such as target
        expo order it is necessary to pass a
        number to parameter `texpo`.

        """

        if method not in _METHOD:
            msg = "Method not implemented"
            raise ValueError(msg)

        if not isinstance(min_size, int) or min_size < 1:
            msg = "Invalid min_size, must be int >=1"
            raise ValueError(msg)

        method = _METHOD[method]
        factor = data.multiplier
        curr = data.currency
        if not curr == _DEFAULT_CURRENCY:
            pair = f"{curr}{_DEFAULT_CURRENCY}"
            factor *= self.__broker.currs[pair].close[0]

        signal = data.signal[0]
        price = data.close[0] * factor
        equity = self.__broker.last_equity

        if np.isnan(price):
            print(f"Data Warn!, {data} is incomplete !!")
            return 0

        if not hasattr(self, "universe"):
            self.universe = self.get_universe()
        if not self.universe:
            print(f"Pipe Warn!, universe is null !!")
            return 0

        if method == _METHOD["EWMA"]:
            vol_target = _DEFAULT_VOLATILITY
            vol_asset = data.volatility[0]
            if data.asset in _MIN_VOL:
                min_vol = _MIN_VOL[data.asset]
                vol_asset = max(vol_asset, min_vol)
            texpo = vol_target / vol_asset

        elif method == _METHOD["EXPO"]:
            assert texpo is not None

        size = signal * texpo * equity / price / len(self.universe)

        if np.isnan(size):
            txt=f"Error found while sizing {data}"
            raise ValueError(txt) 

        if size > 0:
            size = min_size * math.floor(size / min_size)
        elif size < 0:
            size = min_size * math.ceil(size / min_size)

        return size

    def order(
        self,
        data: Optional[Asset] = None,
        size: Optional[Number] = None,
        thresh: float = _DEFAULT_THRESH,
        limit: Optional[float] = None,
        stop: Optional[float] = None,
    ):
        """
        `Order`

        Write description!

        """

        if data is None:
            data = self.asset

        position = self.__broker.get_position(data.ticker)
        current = position.size if position is not None else 0

        if size is None:
            return

        has_position = not current == 0
        has_tresh = thresh > 0

        if has_position and has_tresh:
            stimulus = abs(size) / abs(current)
            if stimulus < thresh:
                return

        self.__broker.new_order(
            data=data,
            size=size,
            limit=limit,
        )

    def order_target(
        self,
        data: Optional[Asset] = None,
        target: Optional[Number] = None,
        thresh: float = _DEFAULT_THRESH,
        limit: Optional[float] = None,
        stop: Optional[float] = None,
    ):
        """
        `Order Target`

        Write description!

        """

        if data is None:
            data = self.asset

        position = self.__broker.get_position(data.ticker)
        current = position.size if position is not None else 0

        if target is None:
            return

        if target == current:
            return

        has_position = not current == 0
        has_tresh = thresh > 0

        delta = target - current

        if has_position and has_tresh:
            stimulus = abs(delta) / abs(current)
            if stimulus < thresh:
                return

        self.__broker.new_order(
            data=data,
            size=delta,
            limit=limit,
        )

    def __repr__(self):
        return f"{self.__class__.__name__} @ {dict(self.__params)}"

    def get_params(self) -> Dict[str, Number]:
        return self.__params

    def get_universe(self) -> Sequence[Asset]:
        return self.__pipeline.universe

    def get_chain(self) -> Sequence[Asset]:
        return self.__pipeline.chain
    
    def get_expo(self) -> Number:
        return self.__broker.get_expo()
    
    def get_texpo(self) -> Number:
        return self.__broker.get_texpo()

    def get_beta(self) -> Number:
        return self.__broker.get_beta()
    
    def get_tbeta(self) -> Number:
        return self.__broker.get_tbeta()

    @property
    def base(self) -> Base:
        return tuple(self.__bases.values())[0]

    @property
    def hbase(self) -> Base:
        return tuple(self.__bases.values())[-1]

    @property
    def bases(self) -> Dict[str, Base]:
        return self.__bases

    @property
    def asset(self) -> Asset:
        return tuple(self.__assets.values())[0]

    @property
    def assets(self) -> Dict[str, Asset]:
        return self.__assets
