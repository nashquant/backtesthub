#! /usr/bin/env python3

from dataclasses import dataclass
import numpy as np
import pandas as pd

from typing import Dict, Any, Optional, Sequence
class Line(np.ndarray):

    """
    Numpy Array Extended Class

    * To find out more about __new__ and __array_finalize__ refer to:
    https://numpy.org/doc/stable/user/basics.subclassing.html

    * Inspired by both Backtesting.py and Backtrader Systems:
    - https://github.com/kernc/backtesting.py.git
    - https://github.com/mementum/backtrader

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

    def __bool__(self):

        try:
            return bool(self[-1])
        
        except KeyError:
            return super().__bool__()

    def __float__(self):

        try:
            return float(self[-1])
        
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
class Data:

    """

    * A data array accessor. Provides access to OHLCV "columns"
    as a standard `pd.DataFrame` would, except it's not a DataFrame
    and the returned "series" are _not_ `pd.Series` but `np.ndarray`
    for performance purposes.

    * Inspired by both Backtesting.py and Backtrader Systems:
    - https://github.com/kernc/backtesting.py.git
    - https://github.com/mementum/backtrader

    """

    __df: pd.DataFrame
    __cache: Dict[str, Line]
    __lines: Dict[str, Line]

    def __init__(self, **kwargs):
        
        self.__df = kwargs.get('df', pd.DataFrame())
        self.__cache = kwargs.get('cache', dict())
        self.__lines = kwargs.get('lines', dict()) 

        self.__start()

    def __start(self):
        
        idx = self.__df.index.copy()
        
        self.__lines = {
            col: Line(array = arr, line = col) \
                for col, arr in self.__df.items()
        }
        
        self.__lines["__index"] = idx

    def __len__(self):
        return self.__len

    def __repr__(self):
        
        idx = min(self.__len, len(self.__df) - 1)
        
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
            return self.__get_line(line)
        
        except:
            msg = f"Line '{line}' non existant"
            raise AttributeError(msg)

    def __getattr__(self, line: str):
        
        try:
            return self.__get_line(line)
        
        except:
            msg = f"Line '{line}' non existant"
            raise AttributeError(msg)

    def __get_line(self, line: str) -> Line:
        
        lobj = self.__cache.get(line)
        
        if lobj is None:
            lobj = self.__lines[line]
            lobj = lobj[:self.__len]
            self.__cache[line] = lobj
        
        return lobj

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
