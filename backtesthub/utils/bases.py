#! /usr/bin/env python3

import numpy as np
import pandas as pd
from datetime import date
from numbers import Number
from typing import Optional, Sequence

from .checks import derive_asset
from .config import (
    _DEFAULT_BUFFER,
    _DEFAULT_CURRENCY,
    _DEFAULT_SLIPPAGE,
    _DEFAULT_SCOMMISSION,
    _DEFAULT_FCOMMISSION,
    _DEFAULT_MATURITY,
    _DEFAULT_HEDGE,
    _HMETHOD,
    _COMMTYPE,
    _RATESLIKE,
    _CURR,
)


class Line(np.ndarray):

    """
    `Line Base Object`

    Line is a ndarray subclass implemented in order to have
    the ability to access entries in a synchronized manner.

    To get a better sense about what is going on, refer to:

    * https://numpy.org/doc/stable/user/basics.subclassing.html
    * https://github.com/mementum/backtrader/blob/master/backtrader/linebuffer.py

    """

    def __new__(cls, array: Sequence = ()):
        arr = np.asarray(array)

        obj = arr.view(cls)
        obj.__array = arr
        obj.__len = len(arr)
        obj.__buffer = _DEFAULT_BUFFER

        return obj

    def __getitem__(self, key: int):
        key += self.__buffer
        return super().__getitem__(key)

    def __repr__(self):
        beg = _DEFAULT_BUFFER
        end = self.__buffer
        return repr(self.__array[beg : end + 1])

    def next(self):
        self.__buffer += 1

    @property
    def buffer(self) -> int:
        return self.__buffer

    @property
    def array(self) -> Sequence:
        return self.__array

    @property
    def series(self) -> pd.Series:
        idx = np.arange(len(self))
        return pd.Series(self.array, idx)

class Data:

    """
    `Data Base Object`

    A data array accessor. Even though the input is a `pd.DataFrame`,
    it gets transformed into a series of Lines (`np.ndarray` subclass)
    due to performance purposes.

    * Inspired by both Backtesting.py and Backtrader Systems:
      - https://github.com/kernc/backtesting.py.git
      - https://github.com/mementum/backtrader

    Data is stored in [lower-cased] columns whose index must be either
    a date or datetime (it will be converted to date).

    Note: The framework still doesn't accept timeframes different than
    "daily"... In future updates we'll tackle this issue.

    """

    def __init__(
        self,
        data: pd.DataFrame,
        index: Sequence[date] = None,
    ):
        if not isinstance(data, pd.DataFrame):
            msg = "Data must be `pd.DataFrame`"
            raise TypeError(msg)

        if not len(data):
            msg = "Cannot accept an empty `pd.DataFrame`"
            raise ValueError(msg)

        if index is not None:
            data = data.reindex(
                index=index,
                method="ffill",
            )
            data = data.sort_index(
                ascending=True,
            )

        else:
            index_type = data.index.inferred_type

            if index_type == "datetime64":
                data.index = data.index.date

            elif not index_type == "date":
                msg = "Index must be `date` or `datetime`"
                raise TypeError(msg)

            data = data.sort_index(
                ascending=True,
            )
            index = tuple(data.index)

        self.__lines = {l.lower(): Line(arr) for l, arr in data.items()}
        self.__lines["__index"] = Line(array=index)
        self.__buffer = _DEFAULT_BUFFER
        self.__df = data

    def __repr__(self):
        dct = {k: v for k, v in self.__df.iloc[self.__buffer].items()}
        lines = ", ".join("{}={:.2f}".format(k, v) for k, v in dct.items())

        return f"<{self.__class__.__name__} {self.ticker} ({self.date}) {lines}>"

    def __getitem__(self, line: str):
        return self.__lines.get(line.lower())

    def __getattr__(self, line: str):
        return self.__lines.get(line.lower())

    def __len__(self):
        return len(self.__df)

    def next(self):
        self.__buffer += 1
        for line in self.__lines:
            self.__lines[line].next()

    def add_line(self, name: str, line: Line):
        if not isinstance(line, Line):
            msg = f"{name} must be Line Type"
            raise TypeError(msg)

        if len(line) != len(self.index):
            msg = "Line must be of same length of Data"
            raise ValueError(msg)

        self.__lines.update(
            {name: line},
        )

    @property
    def index(self) -> Line:
        return self.__lines["__index"]

    @property
    def date(self) -> str:
        return self.__lines["__index"][0]

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    @property
    def schema(self) -> Sequence[str]:
        return tuple(col.lower() for col in self.df.columns)

    @property
    def buffer(self) -> int:
        return self.__buffer

    @property
    def lines(self) -> Sequence[str]:
        lines = self.__lines.keys()
        return tuple(l for l in lines if not l.startswith("__"))


class Base(Data):

    """
    `Base Object`

    Base extends `Data` to create an unique asset class
    that is intended to hold asset/price data that is not
    supposed to be used for trading purposes, but rather
    assets that are used to generate signals, calculate
    currency conversion, and stuff like that.

    Therefore there's no need to define properties such as
    multiplier, commission scheme, etc.

    Note: The name base might be confusing... Please note
    that the name base is different from "Base Class".
    """

    def __init__(
        self,
        ticker: str,
        data: pd.DataFrame,
        index: Sequence[date] = None,
    ):
        self.__ticker = ticker
        super().__init__(
            data=data,
            index=index,
        )

    @property
    def ticker(self) -> str:
        return self.__ticker


class Asset(Base):

    """
    `Asset Class`

    Asset Extends `Base` including features such as ticker,
    currency, commission (value and type), and identifying
    booleans to indicate things such as whether the asset
    is a stock or a future, whether its OHLC data is given
    in [default] price or rates.

    Parameters
    -----------

    `multiplier`: contract base price multiplier, whenever this
      is declared, the data-type is assumed to be futures-like.

    `commission`: broker's commission per trade, may change
      depending on the commission type.

    `commtype` commission type, which is by default set to "PERC"
      to stock-like assets, while "ABS" for future-like ones.

    `slippage` estimated mean slippage per trade. Slippage exists
      to account for the effects of bid-ask spread.

    `currency` asset's quotation currency. Must be among the
      currencies recognized by the algorithm.

    `maturity` derivatives-only parameter. Maturity registers
      the maturity date of the asset, necessary for operations
      such as futures rolling.
    """

    def __init__(
        self,
        ticker: str,
        data: pd.DataFrame,
        index: Sequence[date] = None,
        **commkwargs,
    ):
        super().__init__(
            data=data,
            ticker=ticker,
            index=index,
        )
        self.config(
            **commkwargs,
        )

    def config(
        self,
        commission: Optional[Number] = None,
        multiplier: Optional[Number] = None,
        maturity: date = _DEFAULT_MATURITY,
        slippage: Number = _DEFAULT_SLIPPAGE,
        currency: str = _DEFAULT_CURRENCY,
    ):
        if slippage < 0 or slippage > 1:
            msg = "Invalid value for slippage"
            raise ValueError(msg)

        if currency not in _CURR:
            msg = "Invalid value for currency"
            raise ValueError(msg)

        self.__slippage = slippage
        self.__currency = currency
        self.__maturity = maturity

        if multiplier is None:
            self.__commission = commission or _DEFAULT_SCOMMISSION
            self.__commtype = _COMMTYPE["PERC"]
            self.__multiplier = 1

            self.__stocklike = True
            self.__rateslike = False
            self.__asset = self.ticker

        else:
            self.__commission = commission or _DEFAULT_FCOMMISSION
            self.__commtype = _COMMTYPE["ABS"]
            self.__multiplier = multiplier

            self.__stocklike = False
            self.__asset = derive_asset(self.ticker)
            self.__rateslike = self.__asset in _RATESLIKE

            if maturity == _DEFAULT_MATURITY:
                msg = "Maturity is required for Future-Like assets"
                ValueError(msg)

    @property
    def asset(self) -> str:
        return self.__asset

    @property
    def multiplier(self) -> Number:
        return self.__multiplier

    @property
    def currency(self) -> str:
        return self.__currency

    @property
    def stocklike(self) -> bool:
        return self.__stocklike

    @property
    def rateslike(self) -> bool:
        return self.__rateslike

    @property
    def slippage(self) -> Number:
        return self.__slippage

    @property
    def commission(self) -> Number:
        return self.__commission

    @property
    def commtype(self) -> str:
        return self.__commtype

    @property
    def maturity(self) -> date:
        return self.__maturity

    @property
    def cashlike(self) -> bool:
        return self.__stocklike or self.__rateslike
