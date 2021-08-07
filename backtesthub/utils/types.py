#! /usr/bin/env python3

import numpy as np
import pandas as pd

from numbers import Number
from typing import Optional, Sequence
from .config import _HMETHOD, _COMMTYPE


class Line(np.ndarray):

    """
    Numpy Ndarray Based Concept of Lines

    """

    def __new__(cls, array: Sequence):

        obj = np.asarray(array).view(cls)
        obj.array = np.asarray(array)

        return obj

    def set_buffer(self, bf: int):
        self.__buffer = bf

    def __getitem__(self, key):
        try:
            bkey = key + self.__buffer
            return super().__getitem__(bkey)

        except KeyError:
            return super().__getitem__(key)


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
      Structure as having all properties of an asset, besides
      that we assume the lines can be accessed by the indexation
      of zero-reference (i.e. index 0 access the current point,
      not the first)

    """

    def __init__(self, data: pd.DataFrame):

        self.__df = data
        self.__lines = {}

        idx = self.__df.index.copy()
        self.__lines["__index"] = Line(array = idx)
        self.__lines.update(
            {l.lower(): Line(array=arr) for \
                l, arr in self.__df.items()}
        )

        self.set_buffer(bf = 0)

    def __repr__(self):

        maxlen = len(self.__df) - 1
        i = min(self.__buffer, maxlen)

        index = self.__lines["__index"][i]
        dct = {k: v for k, v in self.__df.iloc[i].items()}
        lines = ", ".join("{}={:.2f}".format(k, v) for k, v in dct.items())

        if hasattr(index, "date"):
            index = index.date()

        if hasattr(index, "isoformat"):
            index = index.isoformat()

        return f"<{self.__class__.__name__} ({index}) {lines}>"

    def set_buffer(self, bf: int):
        self.__buffer = bf

        for line in self.__lines.values():
            line.set_buffer(bf)

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

class Asset(Data):

    """

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

        self.__ticker = ticker
        super().__init__(data=data)

        self.set_properties()

        ## << Implement functions to verify schema conformation!! >> ##

    def set_properties(
        self,
        comm: Optional[Number] = 0,
        ctype: Optional[str] = None,
        mult: Optional[Number] = None,
    ):
        self.__comm = comm

        if mult is not None:
            self.__mult = mult
            self.__stocklike = False
            self.__ctype = ctype or _COMMTYPE["F"]

        else:
            self.__mult = 1
            self.__stocklike = True
            self.__ctype = ctype or _COMMTYPE["S"]

    @property
    def ticker(self):

        return self.__ticker

    @property
    def mult(self):

        return self.__mult

    @property
    def stocklike(self):

        return self.__stocklike

    @property
    def comm(self):

        return self.__comm

    @property
    def ctype(self):

        return self.__ctype


class Hedge(Asset):

    """
    Explain the Class...

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
