#! /usr/bin/env python3

from dataclasses import dataclass
import numpy as np
import pandas as pd

from typing import Dict, Sequence, Any
from .config import _SCHEMA, _COMMTYPE

class Line(np.ndarray):

    """
    Numpy Array Extended Class

    * To find out more about __new__ and __array_finalize__ refer to:
    https://numpy.org/doc/stable/user/basics.subclassing.html

    * This classes were nspired by both projects concepts:
    - https://github.com/kernc/backtesting.py.git -> _Data and _Array
    - https://github.com/mementum/backtrader -> Buffer, Lines, Accessors

    """

    def __new__(cls, array: Sequence, line: str):
        
        obj = np.asarray(array).view(cls)
        
        obj.line = line or getattr(array, "line")
        obj.index = getattr(array, "index")
                
        return obj

    def __array_finalize__(self, obj):
        
        if obj is None: return
        
        self.__line = getattr(obj, "line", "")
        self.__index = getattr(obj, "index", {})

        self._set_buffer()

    def __bool__(self):
        try:
            return bool(self[0])
        
        except KeyError:
            return super().__bool__()

    def __float__(self):
        try:
            return float(self[0])
        
        except KeyError:
            return super().__float__()

    def __repr__(self):
        line = self.__line
        index = self.__index[-1]
        value = self.__float__()
        
        if hasattr(index , "date"): 
            index = index.date()
        
        if hasattr(index, "isoformat"):
            index = index.isoformat()
        
        return f"<Line ({index}) {line} = {value}>"

    def _set_buffer(self, buff:int = 1):
        self.__buff = buff

    def __getitem__(self, key) -> Any:
        try:
            bkey = key + self.__buff - 1
            return super().__getitem__(bkey)
        
        except KeyError:
            return super().__getitem__(key)

    @property
    def s(self) -> pd.Series:
        values = np.atleast_2d(self)
        index = self.__index[:values.shape[1]]
        
        return pd.Series(values[0], index=index, name=self.__line)

    @property
    def df(self) -> pd.DataFrame:
        values = np.atleast_2d(np.asarray(self))
        index = self.__index[:values.shape[1]]
        df = pd.DataFrame(values.T, index=index, columns=[self.__line] * len(values))
        return df


@dataclass
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

    __df: pd.DataFrame
    __lines: Dict[str, Line]
    __margin: float = float("1")
    __mult: float = float("1")
    __ctype: str = _COMMTYPE["S"]
    __comm: float = float("10e-2")
    __slip: float = float("10e-2")
    __stocklike: bool = True
    __hedgelike: bool = False
    __adjusted: bool = True

    def __init__(self, **kwargs):
        
        self.__df = kwargs.get('df', pd.DataFrame())
        self.__lines = kwargs.get('lines', dict())

        self.__inputs = {
            k.lower(): v for k, v in kwargs.items()
        }

        if "mult" in self.__inputs:

            mult = self.__inputs.get('mult')

            self.__stocklike = False
            self.__ctype = _COMMTYPE["F"]
            self.__multiplier = mult 

        self.__start()

    def __start(self):
        
        idx = self.__df.index.copy()
        
        self.__lines = {
            line: Line(array = vals, line = line) \
                for line, vals in self.__df.items()
        }
        
        self.__lines["__index"] = idx

        if not self.__adjusted: self.__adjust()

    def __adjust(self):
        
        """
        * Function that adjust stock data to
          account for dividends and splits.

        * Assumes OHLC data is raw, i.e. without
          any type of adjustment, and assumes that
          Data holds adjreturns Line.  
        
        """

        pass

    def __len__(self):
        return self.__buff

    def _set_buffer(self, buff: int):
        self.__buff = buff

        for line in self.__lines.values():
            line._set_buffer(buff)

    def __repr__(self):
        idx = min(self.__buff, len(self.__df) - 1)
        
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

    ## DataFrame Properties ##

    @property
    def __len(self) -> int:
        return len(self.__df)

    @property
    def df(self) -> pd.DataFrame:
        return self.__df.iloc[: self.__len] if self.__len < len(self.__df) else self.__df

    @property
    def index(self) -> pd.DatetimeIndex:
        return self.__get_line("__index")
