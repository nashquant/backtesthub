#! /usr/bin/env python3

import numpy as np
import pandas as pd

from numbers import Number
from datetime import date, datetime
from typing import Optional, Sequence

from .checks import derive_asset
from .config import _HMETHOD, _COMMTYPE, _SCHEMA


class Line(np.ndarray):

    """
    Numpy Ndarray Based Concept of Lines

    To get a better sense about what is going on, refer to:

    - https://numpy.org/doc/stable/user/basics.subclassing.html
    - https://github.com/mementum/backtrader/blob/master/backtrader/linebuffer.py

    """

    def __new__(cls, array: Sequence):
        arr = np.asarray(array)

        obj = arr.view(cls)
        obj.array = arr

        return obj

    def __getitem__(self, key: int):

        if not hasattr(self, "__buffer"):
            self.__buffer = 0

        key += self.__buffer
        if key < 0:
            msg = "Key l.t zero"
            raise KeyError(msg)

        elif key >= len(self):
            msg = "Key g.t.e length"
            raise KeyError(msg)

        return super().__getitem__(key)

    def __set_buffer(self, bf: int):
        self.__buffer = bf


class Data:

    """
    * A data array accessor. Provides access to OHLCV "columns"
      as a standard `pd.DataFrame` would, except it's not a DataFrame
      and the returned "series" are _not_ `pd.Series` but `np.ndarray`
      for performance purposes.

    * Inspired by both Backtesting.py and Backtrader Systems:
      - https://github.com/kernc/backtesting.py.git
      - https://github.com/mementum/backtrader

    * Different from backtesting.py, we treat the basic Data
      Structure as having all properties of an asset, such as
      ticker, commission structure (type, value), margin req.
      and identifying booleans to indicate things such as
      whether the asset is a stock or a future, whether its
      OHLC data is given in [default] price or rates.

    * Note: The framework still doesn't accept timeframes different
      than `daily`... In future updates we'll tackle this issue.

    """

    def __init__(self, data: pd.DataFrame):
        if not isinstance(data, pd.DataFrame):
            msg = "Data must be `pd.DataFrame`"
            raise NotImplementedError(msg)

        _typ = data.index.inferred_type
        _mon = data.index.is_monotonic_decreasing

        if _typ == "datetime64":
            data.index = data.index.date

        elif not _typ == "date":
            msg = "Index must be `date` or `datetime`"
            raise TypeError(msg)

        if _mon:
            data.sort_index(ascending=True, inplace=True)

        self.__df = data
        self.__lines = {}

        self.__lines["__index"] = Line(array=data.index)
        lines = {l.lower(): Line(arr) for l, arr in data.items()}

        self.__lines.update(lines), self.__reset()

    def __repr__(self):
        maxlen = len(self.__df) - 1
        i = min(self.buffer, maxlen)

        index = self.__lines["__index"][i]
        dct = {k: v for k, v in self.__df.iloc[i].items()}
        lines = ", ".join("{}={:.2f}".format(k, v) for k, v in dct.items())

        if hasattr(index, "date"):
            index = index.date()

        if hasattr(index, "isoformat"):
            index = index.isoformat()

        if self.__class__.__name__ == "Data":
            return f"<{self.__class__.__name__} " f"({index}) {lines}>"
        else:
            return f"<{self.__class__.__name__} " f"{self.ticker} ({index}) {lines}>"

    def __getitem__(self, line: str):
        try:
            return self.__lines.get(line.lower())

        except:
            msg = f"Line '{line}' non existant"
            raise AttributeError(msg)

    def __getattr__(self, line: str):
        try:
            return self.__lines.get(line.lower())

        except:
            msg = f"Line '{line}' non existant"
            raise AttributeError(msg)

    def __len__(self):
        return len(self.__df)

    def __forward(self, step: int = 1):
        self.__buffer = min(
            self.__buffer + step,
            len(self) - 1,
        )

        self.__set_line_buffer()

    def __reset(self, origin: int = 0):
        self.__buffer = min(max(0, origin), len(self) - 1)

        self.__set_line_buffer()

    def __set_line_buffer(self):
        for line in self.__lines.values():
            line._Line__set_buffer(self.__buffer)

    @property
    def buffer(self) -> int:
        return self.__buffer

    @property
    def index(self) -> Line:
        return self.__lines["__index"]

    @property
    def schema(self) -> Sequence[str]:
        lines = self.__lines.keys()
        return [l for l in lines if not l.startswith("__")]


class Base(Data):

    """

    This class is a `Data` child.

    It is intended to hold assets' data 
    that are not supposed to be used for 
    trading purposes, but rather assets
    that are used to generate signals,
    calculate currency conversion, and
    stuff like that.

    Therefore there's no need to define
    properties such as mult, comm, etc.

    """

    def __init__(self, ticker: str, data: pd.DataFrame):
        super().__init__(data=data)
        self.__ticker = ticker
        self.__signal = None

    @property
    def ticker(self) -> str:
        return self.__ticker


class Asset(Base):

    """

    Asset Extends `Base` including features such as:

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

    """

    def __init__(self, ticker: str, data: pd.DataFrame):
        super().__init__(data=data, ticker=ticker)

        self.__asset: str = None
        self.__maturity: date = None

        self.config()

    def config(
        self,
        comm: Number = 0,
        margin: Number = 1,
        mult: Optional[Number] = None,
        currency: str = "BRL",
    ):
        if comm >= 0:
            self.__comm = comm
        else:
            msg = "Invalid value for comm"
            raise ValueError(msg)

        if margin >= 0:
            self.__margin = margin
        else:
            msg = "Invalid value for margin"
            raise ValueError(msg)

        if mult is None:
            self.__mult = 1
            self.__margin = margin
            self.__stocklike = True
            self.__asset = self.__ticker
            self.__ctype = _COMMTYPE["S"]

        else:
            self.__mult = mult
            self.__margin = margin
            self.__stocklike = False
            self.__asset = derive_asset(self.__ticker)
            self.__ctype = _COMMTYPE["F"]

    @property
    def asset(self) -> str:
        return self.__asset

    @property
    def mult(self) -> Number:
        return self.__mult

    @property
    def stocklike(self) -> bool:
        return self.__stocklike

    @property
    def comm(self) -> Number:
        return self.__comm

    @property
    def ctype(self) -> str:
        return self.__ctype

    @property
    def leverage(self) -> Number:
        return 1 / self.__margin

    @property
    def maturity(self) -> date:
        return self.__maturity


class Hedge(Asset):

    """
    Hedge Asset have their own sizing 
    rules, which are dependent on the
    assets that compose the "long" side,
    the hedging method (`hmethod`) and 
    the hedging asset.

    They are treated separately to
    highlight the difference, making
    possible to rapidly identify whether
    a given data structure is an `Asset` 
    or `Hedge` instance.

    """

    def __init__(
        self,
        ticker: str,
        data: pd.DataFrame,
        hmethod: str = _HMETHOD["E"],
    ):
        super().__init__(
            self,
            ticker=ticker,
            data=data,
        )

        self.__hmethod = hmethod

    @property
    def hmethod(self):
        return self.__hmethod
