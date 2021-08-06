#! /usr/bin/env python3

import numpy as np
import pandas as pd

from numbers import Number
from typing import Optional, Sequence
from .config import _SCHEMA, _COMMTYPE

class Line(np.ndarray):

    """
    Numpy Array Extended Class

    * To find out more about __new__ and __array_finalize__ refer to:
    https://numpy.org/doc/stable/user/basics.subclassing.html

    """

    def __new__(cls, array: Sequence, line: str):
        
        obj = np.asarray(array).view(cls)
        obj.line = line or getattr(array, "line")
                
        return obj

    def __array_finalize__(self, obj):
        
        if obj is None: return

        self.set_buffer(bf = 0)

    def set_buffer(self, bf: int):
        self.__buffer = bf

    def __getitem__(self, key):
        try:
            bkey = key + self.__buffer
            return super().__getitem__(bkey)
        
        except KeyError:
            return super().__getitem__(key)


class Asset:

    """

    * A data array accessor. Provides access to OHLCV "columns"
      as a standard `pd.DataFrame` would, except it's not a DataFrame
      and the returned "series" are _not_ `pd.Series` but `np.ndarray`
      for performance purposes.

    * `mult` is the contract base price multiplier,
      whenever this is declared, the data-type is 
      assumed to be futures-like.
    
    * `comm` is the broker's commission per trade

    * `ctype` is the commission type, which are by
      default set to "PERC" to stock-like assets (S), 
      while "ABS" for future-like ones (F).

    * `slip` is the estimated mean slippage per trade.
      Slippage exists to account for the effects of bid-ask
      spread.

    * `margin` is the required margin (ratio) of a leveraged
      account. No difference is made between initial and
      maintenance margins.

    * Inspired by both Backtesting.py and Backtrader Systems:
      - https://github.com/kernc/backtesting.py.git
      - https://github.com/mementum/backtrader

    * Different from backtesting.py, we treat the basic Data 
      Structure as having all properties of an asset, besides 
      that we assume the lines can be accessed by the indexation 
      of zero-reference (i.e. index 0 access the current point, 
      not the first) 

    """

    def __init__(
        self, 
        ticker: str, 
        dataframe: pd.DataFrame
    ):

        self.__ticker = ticker
        self.__df = dataframe

        idx = self.__df.index.copy()
        
        self.__lines = {
            line: Line(array = vals, line = line) \
                for line, vals in self.__df.items()
        }
        
        self.__lines["__index"] = idx

        ## << Implement functions to verify schema conformation >> ##

    def set_properties(
        self, 
        comm: Number,
        mult: Optional[Number] = None,
        ctype: Optional[str] = None,
         
    ):
        self.__comm = comm

        if mult is not None:
            self.__mult = mult
            self.__stocklike = False
            self.__ctype = _COMMTYPE["F"]

        else:
            self.__mult = 1
            self.__stocklike = True
            self.__ctype = _COMMTYPE["S"]

    def adjust(self):
        
        """
        * Function that adjust stock data to
          account for dividends and splits.

        * Assumes OHLC data is raw, i.e. without
          any type of adjustment, and assumes that
          Data holds adjreturns Line.  
        
        """

        pass

    def _set_buffer(self, bf: int):
        self.__buffer = bf

        for line in self.__lines.values():
            line.set_buffer(bf)

    def __repr__(self):
        idx = min(self.__buffer, len(self.__df) - 1)
        
        dct = {k:v for k, v in self.__df.iloc[idx].items()}
        lines = ", ".join("{}={:.2f}".format(k,v) for k, v in dct.items())

        index = self.__lines["__index"][idx]
        
        if hasattr(index , "date"): 
            index = index.date()
        
        if hasattr(index, "isoformat"):
            index = index.isoformat()
        
        return f"<Data ({index}) {lines}>"

    def __getitem__(self, line: str):
        try:
            return self.__lines.get(line)
        
        except:
            msg = f"Line '{line}' non existant"
            raise AttributeError(msg)

    def __getattr__(self, line: str):
        try:
            return self.__lines.get(line)
        
        except:
            msg = f"Line '{line}' non existant"
            raise AttributeError(msg)
