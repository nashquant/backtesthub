import pandas as pd
import numpy as np

from typing import Union
from ..utils.bases import (
    Base,
    Asset,
)

from .ta import (
    KAMAIndicator as KAMA,
    BollingerBands as BBANDS,
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


def SMARatio(
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

    return np.divide(sma1, sma2) - 1


def RevSMACross(
    data: Union[Base, Asset],
    p1: int,
    p2: int,
    *args,
) -> pd.Series:
    """
    `Reversed Simple Moving Average (SMA) Cross`
    """

    sma1 = pd.Series(data.close).rolling(p1).mean()
    sma2 = pd.Series(data.close).rolling(p2).mean()

    return np.sign(sma2 - sma1)


def EMACross(
    data: Union[Base, Asset],
    p1: int,
    p2: int,
    *args,
) -> pd.Series:
    """
    `Exponential Moving Average (EMA) Cross`
    """

    ema1 = pd.Series(data.close).ewm(span=p1).mean()
    ema2 = pd.Series(data.close).ewm(span=p2).mean()

    return np.sign(ema1 - ema2)


def KAMACross(
    data: Union[Base, Asset],
    window: int,
    p1: int,
    p2: int,
    s1: int,
    *args,
) -> pd.Series:
    """
    `Kaufmann's Adaptive Moving Average (KAMA) Cross`
    """

    close = pd.Series(data.close.array)
    
    kama1 = KAMA(close, window=window, pow1 = p1, pow2 = s1)
    kama2 = KAMA(close, window=window, pow1 = p2, pow2 = s1)

    return np.sign(kama1._kama - kama2._kama)

def BBANDSCross(
    data: Union[Base, Asset],
    p: int,
    sma: int,
    stop: int = 0,
    dev: int = 1,
    *args,
) -> pd.Series:
    """
    `Bollinger Bands' (BBANDS) Cross`

    Obs: Stop signal not implemented yet.
    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    smah, smal = high.rolling(sma).mean(), low.rolling(sma).mean()
    bbands = BBANDS(close, window=p, window_dev = dev)

    length = len(close)
    signal = np.zeros(length)
    
    for i in range(1, length):
        if smah[i] >= bbands._hband[i]:
            signal[i] = 1
        elif smal[i] <= bbands._lband[i]:
            signal[i] = -1
        else:
            signal[i] = signal[i-1]

