#! /usr/bin/env python3

import numpy as np
import pandas as pd

from typing import Dict

__all__ = ["_Indicator", "_Array", "_Data"]

class _Array(np.ndarray):

    """
    Numpy Array Extended Class

    Adapted from Backtesting.py
    https://github.com/kernc/backtesting.py.git
    
    """

    def __new__(cls, array, *, name=None, **kwargs):
        obj = np.asarray(array).view(cls)
        obj.name = name or array.name
        return obj

    def __array_finalize__(self, obj):
        if obj is not None:
            self.name = getattr(obj, "name", "")

    def __setstate__(self, state):
        self.__dict__.update(state[-1])
        super().__setstate__(state[:-1])

    def __bool__(self):
        try:
            return bool(self[-1])
        except IndexError:
            return super().__bool__()

    def __float__(self):
        try:
            return float(self[-1])
        except IndexError:
            return super().__float__()

    @property
    def s(self) -> pd.Series:
        values = np.atleast_2d(self)
        index = self._opts["index"][: values.shape[1]]
        return pd.Series(values[0], index=index, name=self.name)

    @property
    def df(self) -> pd.DataFrame:
        values = np.atleast_2d(np.asarray(self))
        index = self._opts["index"][: values.shape[1]]
        df = pd.DataFrame(values.T, index=index, columns=[self.name] * len(values))
        return df


class _Indicator(_Array):
    """
    
    Indicator Base Class.

    Adapted from Backtesting.py
    https://github.com/kernc/backtesting.py.git

    """

class _Data:
    
    """

    A data array accessor. Provides access to OHLCV "columns"
    as a standard `pd.DataFrame` would, except it's not a DataFrame
    and the returned "series" are _not_ `pd.Series` but `np.ndarray`
    for performance reasons.

    Adapted from Backtesting.py
    https://github.com/kernc/backtesting.py.git

    """

    def __init__(self, df: pd.DataFrame):
        self.__df = df
        self.__i = len(df)
        self.__cache: Dict[str, _Array] = {}
        self.__arrays: Dict[str, _Array] = {}
        self._update()

    def __getitem__(self, item):
        return self.__get_array(item)

    def __getattr__(self, item):
        try:
            return self.__get_array(item)
        except KeyError:
            raise AttributeError(f"Column '{item}' not in data") from None

    def _set_length(self, i):
        self.__i = i
        self.__cache.clear()

    def _update(self):
        index = self.__df.index.copy()
        self.__arrays = {
            col: _Array(arr, index=index) for col, arr in self.__df.items()
        }
        # Leave index as Series because pd.Timestamp nicer API to work with
        self.__arrays["__index"] = index

    def __repr__(self):
        i = min(self.__i, len(self.__df) - 1)
        index = self.__arrays["__index"][i]
        items = ", ".join(f"{k}={v}" for k, v in self.__df.iloc[i].items())
        return f"<Data i={i} ({index}) {items}>"

    def __len__(self):
        return self.__i

    @property
    def df(self) -> pd.DataFrame:
        return self.__df.iloc[: self.__i] if self.__i < len(self.__df) else self.__df

    def __get_array(self, key) -> _Array:
        arr = self.__cache.get(key)
        if arr is None:
            arr = self.__cache[key] = self.__arrays[key][: self.__i]
        return arr

    @property
    def Open(self) -> _Array:
        return self.__get_array("Open")

    @property
    def High(self) -> _Array:
        return self.__get_array("High")

    @property
    def Low(self) -> _Array:
        return self.__get_array("Low")

    @property
    def Close(self) -> _Array:
        return self.__get_array("Close")

    @property
    def Volume(self) -> _Array:
        return self.__get_array("Volume")

    @property
    def index(self) -> pd.DatetimeIndex:
        return self.__get_array("__index")

    # Make pickling in Backtest.optimize() work with our catch-all __getattr__
    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state