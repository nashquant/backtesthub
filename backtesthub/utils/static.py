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

    def __new__(cls,  **kwargs: Dict[str, Any]):
        
        array: Sequence = kwargs.pop("array", None)
        line: Optional[str] = kwargs.pop("line", None)
        
        obj = np.asarray(array).view(cls)
        obj.line = line or getattr(array, "line", "")
        obj.kwargs = kwargs
        
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

    A data array accessor. Provides access to OHLCV "columns"
    as a standard `pd.DataFrame` would, except it's not a DataFrame
    and the returned "series" are _not_ `pd.Series` but `np.ndarray`
    for performance reasons.

    Adapted from Backtesting.py
    https://github.com/kernc/backtesting.py.git

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
            col: Line(array = arr, index=idx, line = col) \
                for col, arr in self.__df.items()
        }
        
        self.__lines["__index"] = idx

    def __getitem__(self, line: str):

        return self.__get_line(line)

    def __getattr__(self, line: str):
        
        try:
            return self.__get_line(line)
        
        except KeyError:
            raise AttributeError(f"Line '{line}' non existant")

    def __get_line(self, line: str) -> Line:
        
        lobj = self.__cache.get(line)
        
        if lobj is None:
            lobj = self.__lines[line]
            lobj = lobj[:self.__len]
            self.__cache[line] = lobj
        
        return lobj

    def __len__(self):
        return self.__len

    def __repr__(self):
        idx = min(self.__len, len(self.__df) - 1)
        index = self.__lines["__index"][idx]
        items = ", ".join(f"{k}={v}" for k, v in self.__df.iloc[idx].items())
        
        return f"<Data i={idx} ({index}) {items}>"

    ## DataFrame Properties ##

    @property
    def __len(self) -> int:

        return len(self.__df)

    @property
    def df(self) -> pd.DataFrame:
        return self.__df.iloc[: self.__l] if self.__l < len(self.__df) else self.__df

    ## Line Accessors ##

    @property
    def Open(self) -> Line:
        return self.__get_line("Open")

    @property
    def High(self) -> Line:
        return self.__get_line("High")

    @property
    def Low(self) -> Line:
        return self.__get_line("Low")

    @property
    def Close(self) -> Line:
        return self.__get_line("Close")

    @property
    def Volume(self) -> Line:
        return self.__get_line("Volume")

    @property
    def index(self) -> pd.DatetimeIndex:
        return self.__get_line("__index")
