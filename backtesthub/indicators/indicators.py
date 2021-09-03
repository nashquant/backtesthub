import pandas as pd
import numpy as np

from typing import Union
from ..utils.bases import (
    Base,
    Asset,
)


def Buy_n_Hold(
    data: Union[Base, Asset],
    *args,
) -> pd.Series:
    """
    Simple Buy-n-Hold Long Strategy
    """
    return np.ones(len(data))

def Sell_n_Hold(
    data: Union[Base, Asset],
    *args,
) -> pd.Series:
    """
    Simple Buy-n-Hold Long Strategy
    """
    return -np.ones(len(data))


def SMACross(
    data: Union[Base, Asset],
    p1: int,
    p2: int,
    *args,
) -> pd.Series:
    """
    `Simple Moving Average (SMA) Cross`
    """

    sma1 = pd.Series(data.close).rolling(p1).mean()
    sma2 = pd.Series(data.close).rolling(p2).mean()

    return np.sign(sma1 - sma2)


def EMACross(
    data: Union[Base, Asset],
    p1: int,
    p2: int,
    *args,
) -> pd.Series:
    """
    `Exponential Moving Average (SMA) Cross`
    """
    pass