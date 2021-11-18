import pandas as pd
import numpy as np

from workdays import workday
from holidays import BR, US

from typing import Union
from ..utils.bases import (
    Base,
    Asset,
)

from .ta import (
    KAMAIndicator as KAMA,
    BollingerBands as BBANDS,
    DonchianChannel as DONCH,
    AverageTrueRange as ATR,
    RSIIndicator as RSI,
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
    p1: int,
    p2: int,
    f1: int,
    f2: int,
    s1: int = 30,
    s2: int = 30,
    *args,
) -> pd.Series:
    """
    `Kaufmann's Adaptive Moving Average (KAMA) Cross`

    e.g. KAMA(10,1,30) vs. KAMA(10,2,30) is the most basic KAMA | param (p:10, p2: 10, f1: 1, f2: 2, s1: 30, s2: 30)

    For more information: 
    https://corporatefinanceinstitute.com/resources/knowledge/trading-investing/kaufmans-adaptive-moving-average-kama/

    """

    close = pd.Series(data.close.array)

    kama1 = KAMA(close, window=p1, pow1=f1, pow2=s1)
    kama2 = KAMA(close, window=p2, pow1=f2, pow2=s2)

    return np.sign(kama1._kama - kama2._kama)


def BBANDSCross(
    data: Union[Base, Asset],
    p: int,
    sma: int,
    dev: int = 1,
    stop: int = 0,
    *args,
) -> pd.Series:
    """
    `Bollinger Bands' (BBANDS) Cross`

    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    smah, smal = high.rolling(sma).mean(), low.rolling(sma).mean()
    bbands = BBANDS(close, window=p, window_dev=dev)
    s = DONCH(high, low, close, window=stop)

    length = len(close)
    signal = np.zeros(length)

    for i in range(1, length):
        if smah[i] >= bbands._hband[i] and smah[i-1] < bbands._hband[i-1]:
            signal[i] = 1
        elif smal[i] <= bbands._lband[i] and smal[i-1] > bbands._lband[i-1]:
            signal[i] = -1
        else:
            signal[i] = signal[i - 1]

        if stop and signal[i] == 1:
            if low[i] <= s._lband[i]:
                signal[i] = 0
        elif stop and signal[i] == -1:
            if high[i] >= s._hband[i]:
                signal[i] = 0

    return signal


def Turtle(
    data: Union[Base, Asset],
    p: int,
    stop: int,
    *args,
) -> pd.Series:
    """
    `Turtle Momentum`

    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    donch = DONCH(high, low, close, window=p)
    s = DONCH(high, low, close, window=stop)

    length = len(close)
    signal = np.zeros(length)

    for i in range(1, length):
        if high[i] == donch._hband[i]:
            signal[i] = 1
        elif low[i] == donch._lband[i]:
            signal[i] = -1
        else:
            signal[i] = signal[i-1]

        if stop and signal[i] == 1:
            if low[i] == s._lband[i]:
                signal[i] = 0
        elif stop and signal[i] == -1:
            if high[i] == s._hband[i]:
                signal[i] = 0

    return signal


def Donchian(
    data: Union[Base, Asset],
    p: int,
    sma: int,
    stop: int,
    *args,
) -> pd.Series:
    """
    `Donchian Momentum`

    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    smac = close.rolling(sma).mean()
    donch = DONCH(high, low, close, window=p)
    mid = donch.donchian_channel_mband()

    s = DONCH(high, low, close, window=stop)

    length = len(close)
    signal = np.zeros(length)

    for i in range(1, length):
        if smac[i] >= mid[i] and smac[i-1] < mid[i-1]:
            signal[i] = 1
        elif smac[i] <= mid[i] and smac[i-1] > mid[i-1]:
            signal[i] = -1
        else:
            signal[i] = signal[i-1]

        if stop and signal[i] == 1:
            if low[i] <= s._lband[i]:
                signal[i] = 0
        elif stop and signal[i] == -1:
            if high[i] >= s._hband[i]:
                signal[i] = 0

    return signal


def DonchianATR(
    data: Union[Base, Asset],
    p: int,
    sma: int,
    mult: int,
    stop: int,
    *args,
) -> pd.Series:
    """
    `Donchian ATR Momentum`

    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    atr = ATR(high, low, close)

    smac = close.rolling(sma).mean()

    donch = DONCH(high, low, close, window=p)
    s = DONCH(high, low, close, window=stop)

    length = len(close)
    signal = np.zeros(length)

    for i in range(1, length):
        if smac[i] - donch._lband[i] >= mult*atr._atr[i] and smac[i-1] - donch._lband[i-1] < mult*atr._atr[i-1]:
            signal[i] = 1
        elif donch._hband[i] - smac[i] >= mult*atr._atr[i] and donch._hband[i-1] - smac[i-1] < mult*atr._atr[i-1]:
            signal[i] = -1
        else:
            signal[i] = signal[i-1]

        if stop and signal[i] == 1:
            if low[i] <= s._lband[i]:
                signal[i] = 0
        elif stop and signal[i] == -1:
            if high[i] >= s._hband[i]:
                signal[i] = 0

    return signal


def CRSI(
    data: Union[Base, Asset],
    p: int,
    upper: int,
    lower: int,
    stop: int,
    *args,
) -> pd.Series:
    """
    `Counter-Trend RSI`

    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    rsi = RSI(close, window=p)
    s = DONCH(high, low, close, window=stop)

    length = len(close)
    signal = np.zeros(length)

    for i in range(1, length):
        if rsi._rsi[i] <= lower and rsi._rsi[i-1] > lower:
            signal[i] = 1
        elif rsi._rsi[i] >= upper and rsi._rsi[i-1] < upper:
            signal[i] = -1
        else:
            signal[i] = signal[i-1]

        if stop and signal[i] == 1:
            if low[i] <= s._lband[i]:
                signal[i] = 0
        elif stop and signal[i] == -1:
            if high[i] >= s._hband[i]:
                signal[i] = 0

    return signal


def CBBANDS(
    data: Union[Base, Asset],
    p: int,
    dev: int = 2,
    stop: int = 0,
    *args,
) -> pd.Series:
    """
    `Counter-Trend BBANDS`

    """

    high = pd.Series(data.high.array)
    close = pd.Series(data.close.array)
    low = pd.Series(data.low.array)

    bbands = BBANDS(close, window=p, window_dev=dev)
    s = DONCH(high, low, close, window=stop)

    length = len(close)
    signal = np.zeros(length)

    for i in range(1, length):
        if bbands._lband[i] >= close[i] and bbands._lband[i-1] < close[i-1]:
            signal[i] = 1
        elif bbands._hband[i] <= close[i] and bbands._hband[i-1] > close[i-1]:
            signal[i] = -1
        else:
            signal[i] = signal[i-1]

        if stop and signal[i] == 1:
            if low[i] <= s._lband[i]:
                signal[i] = 0
        elif stop and signal[i] == -1:
            if high[i] >= s._hband[i]:
                signal[i] = 0

    return signal


def CROSSCORREL(
    data: Union[Base, Asset],
    base: Union[Base, Asset],
    lag: int = 1,
    alpha: int = 0.8,
    minRet: float = 0.01,
    minCorr: float = 0.05,
    *args,
) -> pd.Series:
    """
    `Condition Variable`

    """

    d = _resample_week(data)
    r = d.close.pct_change()

    b = _resample_week(base)
    rb = b.close.pct_change()
    corr = r.ewm(alpha=alpha).corr(rb.shift(lag))

    abs_rb, abs_corr = abs(rb), abs(corr)

    rsig = np.sign(
        abs_rb.where(
            abs_rb > minRet,
            0
        )
    )

    csig = np.sign(
        abs_corr.where(
            abs_corr > minCorr,
            0
        )
    )

    df = rsig * csig

    return df.reindex(data.index, method='ffill')


def _resample_week(
    data: Union[Base, Asset],
) -> pd.Series:
    """
    `Resample Daily->Weekly`

    Here we use this function as an utils to convert a
    dataframe from daily frequency to weekly frequency.

    The script below would make our lives much easier if
    it worked. The problem is that for our script, we
    want to get the weekly last close as our price (i.e., 
    usually happens on friday, except when there is a holiday... 
    This exception disable the effectiveness of the script below.)

    ###################################
    df = data.df
    df.index = pd.to_datetime(df.index)
    w = df.resample('W-Fri').last()
    w = w[w.index<=df.index[-1]]

    return w
    ###################################

    This utils function has BR calendar hardcoded because it is
    not intended to be customizable yet. In future updates this
    behavior may be altered. 

    """
    df = data.df
    df.index = pd.to_datetime(df.index)

    years = [y for y in range(df.index[-1].year-1, df.index[-1].year+1)]
    calendar = BR(state='SP', years=years)

    holidays = list(calendar.keys())

    df['dates'] = df.index
    df['next'] = df.dates.shift(-1)
    df['next'][-1] = workday(df.dates[-1], 1, holidays=holidays)

    mask = df.dates.apply(lambda x: x.weekday()) > df.next.apply(
        lambda x: x.weekday())
    return df[mask][['open', 'high', 'low', 'close']]
