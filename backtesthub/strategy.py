#! /usr/bin/env python3

import math
import numpy as np
from numbers import Number
from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, Union, Optional, Sequence

from .broker import Broker
from .pipeline import Pipeline
from .indicators import Default
from .utils.math import EWMAVolatility
from .utils.bases import Line, Base, Asset, Hedge
from .utils.config import (
    _DEFAULT_VOLATILITY,
    _DEFAULT_MIN_SIZE,
    _DEFAULT_CURRENCY,
    _DEFAULT_SIZING,
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
        """ """
    @abstractmethod
    def next():
        """ """

    def I(
        self,
        data: Union[Base, Asset, Hedge],
        func: Callable = Default,
        *args: Number,
    ):
        """
        `Indicator Assignment`

        - Takes a custom function, a data structre
          that can either be Base, Asset or Hedge,
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
        """
        `Volatility Assignment`

        Basically does the same job as `self.I`
        but it is applied to volatility calcs.  
        """
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
        """
        `Broadcasting Lines`

        Very important object that allows one
        to assign the indicator/signal lines
        of an object to another one.

        Therefore, it allows one to calculate
        signals/volatility/indicators using a 
        `Base` data, and transfer the results
        to one or multiple other `Assets` data.
        """
        base_lines = set(base.lines)
        
        if not lines: 
            new_lines = ["signal", "volatility"]
        else:
            new_lines = lines
            
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

    def order_target(
        self,
        data: Union[Asset, Hedge],
        target: Optional[float] = None,
        method: str = _DEFAULT_SIZING,
        thresh: float = _DEFAULT_THRESH,
        min_size: int = _DEFAULT_MIN_SIZE,
        limit: Optional[float] = None,
        stop: Optional[float] = None,
    ) -> Number:
        """
        `Order Target Generation`

        Very important object that allows one
        to create with great flexibility an 
        order to be sent to the broker.

        First thing is to correctly select 
        the method to be employed.
        
        Default: Sizing is done with inverse
        volatility sizing.
        
        Obs: For other methods, such as target
        expo order it is necessary to pass a
        number to parameter `target`.

        Then, the user can manipulate the order
        by assigning other behavior such as:

        1) `Threshold`: order is trigged only if 
           the position is not opened, else if
           the "delta" is a least "thresh" times
           the current position size.
        
        2) `Min_size`: ...

        3) `Limit_price`: ...

        4) `Stop_price`: ...
        
        """
        if method not in _METHOD:
            msg="Method not implemented"
            raise ValueError(msg)
        
        if type(min_size)!= int or min_size < 1:
            msg="Invalid min_size, must be int >=1"
            raise ValueError(msg) 

        current = self.get_current(data)
        equity = self.__broker.last_equity
        method = _METHOD[method]

        factor = data.multiplier
        curr = data.currency
        if not curr == _DEFAULT_CURRENCY:
            pair = f"{curr}{_DEFAULT_CURRENCY}"
            factor *= self.__broker.currs[pair].close[0] 

        signal = data.signal[0]
        price = data.close[0] * factor

        if method == _METHOD["EWMA"]:
            vol_target = _DEFAULT_VOLATILITY
            vol_asset = data.volatility[0]
            target = vol_target/ vol_asset 

            size = signal * target * equity / price

        elif method == _METHOD["EXPO"]:
            if target is None:
                msg="Need to assign a Target"
                raise ValueError(msg)

            size = signal * target * equity / price

        else:
            msg = "Method still not Implemented"
            raise NotImplementedError(msg)
        
        if size > 0:
            size = min_size*math.floor(size/min_size)
        elif size < 0:
            size = min_size*math.ceil(size/min_size)

        if size==current:
            return

        has_position = (not current == 0)
        has_tresh = (thresh > 0) 

        delta = size - current

        if has_position and has_tresh:
            stimulus = abs(delta) / abs(current)
            if stimulus < thresh:
                return

        self.__broker.new_order(
            data=data,
            size=delta,
            limit=limit,
        )

        return delta

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