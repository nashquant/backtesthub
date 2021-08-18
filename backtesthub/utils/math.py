#! /usr/bin/env python3

import pandas as pd
from numbers import Number
from math import sqrt
from typing import Union

from backtesthub.utils import Base, Asset, Hedge
from backtesthub.utils.config import (
    _DEFAULT_VPARAM,
    _VMETHOD,
)

def EWMA(
    data: Union[Base, Asset, Hedge],
    method: str = _VMETHOD["EWMA"],
    alpha: float = _DEFAULT_VPARAM,
) -> pd.Series:

    if not type(data) in (Base, Asset, Hedge):
        msg ="Wrong data type input"
        raise TypeError(msg)

    schema = set(data.schema)
    
    if "close" not in schema:
        msg="Close not in Schema"
        raise ValueError(msg)

    return data.close.series.ewm(alpha = alpha).mean()

def EWMAVolatility(
    data: Union[Base, Asset, Hedge],
    method: str = _VMETHOD["EWMA"],
    alpha: float = _DEFAULT_VPARAM,
    freq: Number = 252, 
) -> pd.Series:

    if not type(data) in (Base, Asset, Hedge):
        msg ="Wrong data type input"
        raise TypeError(msg)

    schema = set(data.schema)

    if "returns" in schema:
        returns = data.returns.series
    elif "close" in schema:
        returns = data.close.series.pct_change()
    else:
        msg="Close not in Schema"
        raise ValueError(msg)

    return returns.ewm(alpha = alpha).std() * sqrt(freq)
