#! /usr/bin/env python3

import numpy as np
import pandas as pd
from .utils.static import *
from dataclasses import dataclass
from warnings import filterwarnings
from abc import ABCMeta, abstractmethod 
from typing import List, Dict, Callable

from .position import *
from .broker import *
from .order import *

filterwarnings('ignore')

@dataclass
class Strategy(metaclass = ABCMeta):
    
    """
    
    """
    __dnames: Dict[str, Data]
    __broker: Broker
    __params: Dict

    @abstractmethod
    def init(self):
        """
        Initialize the strategy.
        Override this method.
        Declare indicators (with `backtesting.backtesting.Strategy.I`).
        Precompute what needs to be precomputed or can be precomputed
        in a vectorized fashion before the strategy starts.
        If you extend composable strategies from `backtesting.lib`,
        make sure to call:
            super().init()
        """

    @abstractmethod
    def next(self):
        """
        Main strategy runtime method, called as each new
        `backtesting.backtesting.Strategy.data`
        instance (row; full candlestick bar) becomes available.
        This is the main method where strategy decisions
        upon data precomputed in `backtesting.backtesting.Strategy.init`
        take place.
        If you extend composable strategies from `backtesting.lib`,
        make sure to call:
            super().next()
        """

    @property
    def equity(self) -> float:
        
        """
        Current account equity 
        """
        
        return self.__broker.equity

    @property
    def data(self) -> Data:
        """
        
        * `data` is _not_ a DataFrame, but a custom structure
          that serves customized numpy arrays for reasons of performance
          and convenience. Besides OHLCV columns, `.index` and length,
          it offers `.pip` property, the smallest price unit of change.

        * Within `backtesting.backtesting.Strategy.init`, `data` arrays
          are available in full length, as passed into
          `backtesting.backtesting.Backtest.__init__`
          (for precomputing indicators and such). However, within
          `backtesting.backtesting.Strategy.next`, `data` arrays are
          only as long as the current iteration, simulating gradual
          price point revelation. In each call of
          `backtesting.backtesting.Strategy.next` (iteratively called by
          `backtesting.backtesting.Backtest` internally),
          the last array value (e.g. `data.Close[-1]`)
          is always the _most recent_ value.

        * If you need data arrays (e.g. `data.Close`) to be indexed
          **Pandas series**, you can call their `.s` accessor
          (e.g. `data.Close.s`). 
          
        
        """
        return self.__dnames['ticker']

    @property
    def position(self) -> List[Position]:
        """Instance of `backtesting.backtesting.Position`."""
        return self.__broker.position

    @property
    def orders(self) -> List[Order]:
        """List of orders (see `Order`) waiting for execution."""
        return Order(self.__broker.orders)


    def I(self, f: Callable, *args, **kwargs) -> Line:
        
        """
        Declare indicator. 
        
        An indicator is just an array of values,but one that is revealed 
        gradually in `backtesting.backtesting.Strategy.next` much like
        `backtesting.backtesting.Strategy.data` is.
        
        `func` is a function that returns the indicator array(s) of
        same length as `backtesting.backtesting.Strategy.data`.
        
        Additional `*args` and `**kwargs` are passed to `func` and can
        be used for parameters.
        
        For example, using simple moving average function from TA-Lib:
            def init():
                self.sma = self.I(ta.SMA, self.data.Close, self.n_sma)
        """


        try:
            value = f(*args, **kwargs)
        
        except Exception as e:
            raise Exception(e)

        if isinstance(value, pd.DataFrame):
            value = value.values.T

        if value is not None:
            value = np.asarray(value, order='C')
        is_arraylike = value is not None

        # Optionally flip the array if the user returned e.g. `df.values`
        if is_arraylike and np.argmax(value.shape) == 0:
            value = value.T

        if not is_arraylike or not 1 <= value.ndim <= 2 or value.shape[-1] != len(self.__data.Close):
            raise ValueError(
                'Indicators must return (optionally a tuple of) numpy.arrays of same '
                f'length as `data` (data shape: {self.__data.Close.shape};"'
                f'shape: {getattr(value, "shape" , "")}, returned value: {value})')

        value = Line(
            value, 
            index=self.data.index
        )
        
        return value

    def buy(
        self,
        ticker: str,
        size: float,
        price: float
    ):
        
        return self.__broker._issue_order(
            ticker, size, price
        )

    def sell(
        self,
        ticker: str,
        size: float,
        price: float
    ):
        
        return self.__broker._issue_order(
            ticker, -size, price
        )


    