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

    """
    `EWMA Function`
    
    Applies EWMA(alpha = _DEF_VPARAM) to data.close
    in order to return a pd.Series with the exp. mov.
    average
    
    For more information about EWMA and parameters
    refer to the function `EWMA_volatility` below.  
    """

    if not type(data) in (Base, Asset):
        msg = "Wrong data type input"
        raise TypeError(msg)

    schema = set(data.schema)

    if "close" not in schema:
        msg = "Close not in Schema"
        raise ValueError(msg)

    return data.close.series.ewm(alpha=alpha).mean()


def EWMA_volatility(
    data: Union[Base, Asset],
    alpha: float = _DEFAULT_VPARAM,
) -> pd.Series:

    """
    `EWMA Volatility Function`
    
    EWMA stands for Exponentially Weighted Moving Average. 
    
    This algorithm computes volatility with EWMA approach, 
    i.e., vol_t = (1-alpha) * vol_t-1 + alpha(ret_t**2)

    It uses the standard df.ewm().std() implementation,
    in order to compute this in a vectorized fashion.
    Besides that, if alpha is not given, the function
    takes the env variable DEF_VPARAM which starts
    at default value of 5/100.
    
    """

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

    return returns.ewm(alpha=alpha).std() * math.sqrt(252)


def adjust_stocks(data: pd.DataFrame) -> pd.DataFrame:

    """
    `Adjust Stocks Function`

    This "utils" function expects users to give a 
    `pd.DataFrame` with columns at least {"Close", 
    "Returns"} - better if they contain all OHLC = 
    {"Open", "High", "Low", "Close"} + {"Returns"}- 
    
    Additionally, this price  dataframe is assumed to 
    have no adjustments, which means that it registers
    data as it happened in the past ("as is").

    For stocks, in order to be consistent, we need 
    to update those previous values adjusting them
    by dividends and stock splits, or at least we
    would need the `Broker` to be able to handle
    those events, otherwise the calculated PNL 
    would be wrong!

    For the way we process data, we prefer to store
    OHLCV data without any adjustment ("as is"), 
    because it is easier to maintain (no need to 
    recurrent maintenance, as it would if we stored 
    adjusted data). 
    
    Besides that, instead of ETLing a collection/table 
    of events (e.g. dividends and splits), we prefer to 
    store the adjusted returns for each day (which does
    not require constant maintanence) and bear the "not
    so bad" (for us, at least) overhead of adjusting 
    OHLCV each time we run a simulation.  

    NOTE: One may think that the adjustment factors
    might become large enough that the price becomes 
    too small that affects PNL calculations. For all
    stocks we tested (e.g. MGLU3 is an interesting 
    case), we found no issue with that because we put 
    no restrictions to the number of decimals we output 
    after the adjustment.
    """

    schema = list(data.columns)
    OHLC = ["open", "high", "low", "close"]

    data = data.sort_index(ascending=False)
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

    return data[schema]


def rate2price(
    data: pd.DataFrame,
    maturity: date,
    holidays: Sequence[date] = [],
    contract_size: float = float("10e5"),
):

    """
    `Adjust Rates Function`

    This "utils" function expects users to give a 
    `pd.DataFrame` with columns at least {"Close", 
    "Returns"} - better if they contain all OHLC = 
    {"Open", "High", "Low", "Close"} + {"Returns"}- 

    It will then compute the transformation of base
    from rates - e.g. DI1F23 may be quoted at any 
    day, say 2021/09/15, as 8.94%y.y - to price per
    unit - for the last e.g., PRICE = R$ 89,514.32. 
    
    To carry this transformation, we need to have
    a calendar of future holidays (at least up 
    until maturity), and the contract size. 

    NOTE: For this function, we assume those values 
    to be specifically designed to Brazilian DI. If
    one is interested in computing cases other than
    this one, some changes must be implemented! 
    """

    schema = list(data.columns)

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
    """
    `Fill OHLC Function`

    This "utils" function expects users to give a 
    `pd.DataFrame` with columns at least {"Close", 
    "Returns"} - better if they contain all OHLC = 
    {"Open", "High", "Low", "Close"} + {"Returns"}-

    We know, oftentimes, price dataframes aren't 
    completely "clean", which means that specially
    OHL values may be None/NaN, or may contain 
    values that doesn't make sense.

    Thus, this procedure applies, in a vectorized
    fashion, a "data cleansing" check for OHL
    existance (if they don't exist, they will
    be set @ close price) and their validity
    
    H = max(open, high, low, close)
    L = min(open, high, low, close)
    
    By applying this procedure, we can assure that
    if close prices are correct (at least those 
    are the ones we might have more certain about
    their validity), we'll be able to input a 
    consistent price df for the `Broker`.  
    """

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
