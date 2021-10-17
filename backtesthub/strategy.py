#! /usr/bin/env python3

import math
import numpy as np
import pandas as pd
from numbers import Number
from abc import ABCMeta, abstractmethod
from collections import OrderedDict, defaultdict as ddict
from typing import Callable, Dict, Union, Optional, Sequence

from .broker import Broker
from .pipeline import Pipeline
from .utils.math import EWMA_volatility
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

    Strategy is responsible for carrying all
    the strategy logic [through ind/signals
    definition] and trading commands [through
    sizing positions and order issue to Broker].

    This is one of the most complex classes of 
    backtesthub, it leverages on Internal APIs
    to various other classes instances, such as
    `Pipeline`(which is used to get for each run
    the most updated universe), `Broker` (which
    gives information about current net exposure/
    beta for hedging purposes, net equity value,
    etc. and enables a channel to send orders) 
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
        self.__params = None

    @abstractmethod
    def init():
        """
        `Strategy Initialization`

        Since this is an abstract method, it 
        is expected to be overriden by another 
        method belonging to a child class.

        This child class' init method will be 
        responsible for setting up the initial 
        conditions of the strategy object. This 
        may include (not exhaustive): 
        
        1) Indicator Setting/Broadcasting
        2) Volatility Setting/Broadcasting
        3) Universe Initialization
        4) Parameters Setting
        """

    @abstractmethod
    def next():
        """ 
        `Strategy Running`

        Since this is an abstract method, it is expected 
        to be overriden by another method belonging to a 
        child class.

        This child class' next method will be responsible 
        for carrying on all steps necessary for strategy 
        definition each period, which may include a subset 
        of following (not exhaustive) list:

        1) Universe Setting/Updating.
        2) Position Sizing [EXPO vs. IVOL].
        3) Order Issue to Broker.

        OBS: IVOL = INVERSE VOLATILITY, is the default method 
        used by this class to properly size a position. It 
        assumes that a target volatility is assigned(it is, 
        by env variable _DEF_VOL, which is by default 10%) 

        Called first time @ `start date` + `buffer` and 
        recurrently called at each period after as defined 
        by the `global index` set up until it reaches end 
        or the simulation is stopped.

        Refer to backtesthub/calendar.py to know more about 
        global index setting.

        NOTE: It is assumed that Strategy's `next` is responsible 
        for new/current trading, it is not responsible for closing 
        positions that no longer remains in the universe, though.
        """

    def I(
        self,
        data: Union[Base, Asset],
        func: Callable,
        **kwargs: Number,
    ) -> pd.Series:
        """
        `Indicator Assignment Method`

        Takes a custom function, a data structre
        that can either be a Base or an Asset
        and some parameters to be passed to func.

        The function is supposed to receive the
        data structure, and it is expected that
        the function can manipulate the data "schema".

        The function should then perform calculations
        in a Line of the object and return an array-like
        result containing only numbers
        """

        try:
            ind = func(data, *kwargs.values())
        except Exception as e:
            raise Exception(e)

        if not len(data) == len(ind):
            msg = f"Line length not compatible"
            raise ValueError(msg)

        return pd.Series(ind)

    def V(
        self,
        data: Union[Base, Asset],
        func: Callable = EWMA_volatility,
        **kwargs: Union[str, int, float],
    ) -> pd.Series:
        """
        `Volatility Assignment Method`

        Basically does the same job as `self.I`
        but it is applied to volatility calcs.
        """
        
        try:
            vol = func(data, *kwargs.values())
        except Exception as e:
            raise Exception(e)

        return pd.Series(vol)

    def broadcast(
        self,
        base: Base,
        assets: Dict[str, Asset],
        lines: Sequence[str] = [],
    ):
        """
        `Lines Broadcasting Method`

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
        `Order Sizer Method`

        Very important method that allows one
        to compute with great flexibility the
        order size to be sent to the Broker.

        First thing is to correctly select the 
        method to be employed, which by default 
        is the inverse volatility sizing.

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

        if method == _METHOD["EWMA"]:
            vol_target = _DEFAULT_VOLATILITY
            vol_asset = data.volatility[0]
            if data.asset in _MIN_VOL:
                min_vol = _MIN_VOL[data.asset]
                vol_asset = max(vol_asset, min_vol)
            texpo = vol_target / vol_asset

        elif method == _METHOD["EXPO"]:
            assert texpo is not None

        size = signal * texpo * equity / price

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
        `Order Method`

        Issues an order to Broker.

        Order is executed whenever it is an opening
        position (i.e. no previous position exists),
        else if order size is greater than current
        size (both in absolute terms) by a threshold.

        If `threshold` is not set by the user, it will 
        assume the default value set by _DEF_THRESH env 
        variable (initialized at 20%).

        Limit and Stop orders are still not available.

        BEWARE: Users are enabled to call this function
        without assigning any data, which automatically
        sets data to be the first element of the assets
        ordered dictionary.
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
        `Order Target Method`

        Issues an order to Broker, similarly to `order` 
        method, but instead of dealing with size of the 
        order as an input, it gives the total target
        position (which, oftentimes, may be the most 
        convenient). Thus, it simply has an extra step 
        that involves calculating the order size given 
        both target and current positions.

        Limit and Stop orders are still not available.

        BEWARE: Users are enabled to call this function
        without assigning any data, which automatically
        sets data to be the first element of the assets
        ordered dictionary.

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
        return f"{self.__class__.__name__} @ {self.params}"

    def get_params(self) -> Dict[str, Number]:
        if hasattr(self, "params"):
            if isinstance(self.params, dict): 
                return self.params
        elif hasattr(self, "__params"):
            if isinstance(self.__params, dict):
                return self.__params
        else:
            return {} 

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
