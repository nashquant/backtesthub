#! /usr/bin/env python3

import math
import pandas as pd
from holidays import BR
from datetime import date
from workdays import networkdays
from typing import Union, Sequence

from .bases import Base, Asset
from .config import (
    _DEFAULT_VPARAM,
)


def EWMA(
    data: Union[Base, Asset],
    alpha: float = _DEFAULT_VPARAM,
) -> pd.Series:

    if not type(data) in (Base, Asset):
        msg = "Wrong data type input"
        raise TypeError(msg)

    schema = set(data.schema)

    if "close" not in schema:
        msg = "Close not in Schema"
        raise ValueError(msg)

    return data.close.series.ewm(alpha=alpha).mean()


def EWMAVolatility(
    data: Union[Base, Asset],
    alpha: float = _DEFAULT_VPARAM,
    freq: int = 252,
) -> pd.Series:

    if not type(data) in (Base, Asset):
        msg = "Wrong data type input"
        raise TypeError(msg)

    schema = set(data.schema)

    if "returns" in schema:
        returns = data.returns.series
    elif "close" in schema:
        returns = data.close.series.pct_change()
    else:
        msg = "Close not in Schema"
        raise ValueError(msg)

    return returns.ewm(alpha=alpha).std() * math.sqrt(freq)


def adjust_stocks(data: pd.DataFrame) -> pd.DataFrame:
    data = data.sort_index(ascending=False)
    OHLC = ["open", "high", "low", "close"]
    cumprod = (
        (1 + data.returns)
        .cumprod()
        .shift(
            periods=1,
            fill_value=1,
        )
    )

    aclose = data.close[0] / cumprod
    data["multpl"] = aclose / data.close

    for col in OHLC:
        data[col] = data[col] * data.multpl

    data.drop(columns="multpl", inplace=True)
    data = data.sort_index(ascending=True)

    return data[OHLC]


def rate2price(
    data: pd.DataFrame,
    maturity: date,
    holidays: Sequence[date] = [],
    contract_size: float = float("10e5"),
):

    """
    Receives a dataframe (OHLC schemed) to
    make rate-price transformation, based
    upon a given set of holidays.

    <<<ATTENTION: CODE NEEDS REFACTORING>>>>
    <<<REMINDER: CHANGE HIGH-LOW ORDER>>>>>>

    """

    schema = ["open", "high", "low", "close"]

    if not holidays:
        calendar = BR(years=[y for y in range(1990, 2100)])
        holidays = tuple(calendar.keys())

    pu = (1 + data.divide(100)).pow(1 / 252)
    pu['date'] = pu.index

    pu["net_days"] = pu.date.apply(
        lambda dt: networkdays(
            dt,
            maturity,
            holidays,
        )
    )

    pu["size"] = contract_size

    for col in schema:
        pu[col] = pu["size"].div(pu[col].pow(pu["net_days"] + 1))

    return pu.rename(columns={"high":"low", "low": "high"})[schema]


def fill_OHLC(df: pd.DataFrame) -> pd.DataFrame:

    if "close" not in df.columns:
        txt = "df must have at least CLOSE as column"
        raise ValueError(txt)

    if "open" in df.columns:
        df.loc[df.open.isna(), "open"] = df.loc[df.open.isna(), "close"]
    else:
        df["open"] = df["close"]

    if "high" in df.columns:
        df.loc[df.high.isna(), "high"] = df.loc[df.high.isna(), ["open", "close"]].max(
            axis=1
        )
    else:
        df["high"] = df[["open", "close"]].max(axis=1)

    if "low" in df.columns:
        df.loc[df.low.isna(), "low"] = df.loc[df.low.isna(), ["open", "close"]].min(
            axis=1
        )

    else:
        df["low"] = df[["open", "close"]].min(axis=1)

    return df
