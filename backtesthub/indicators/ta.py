import pandas as pd
import numpy as np
import math

"""
Technical Indicators Library

From TA library project (https://github.com/bukosabino/ta)
Original Author: Dario Lopez Padial (Bukosabino)

"""


class IndicatorMixin:
    """Util mixin indicator class"""

    _fillna = False

    def _check_fillna(self, series: pd.Series, value: int = 0) -> pd.Series:
        """Check if fillna flag is True.
        Args:
            series(pandas.Series): dataset 'Close' column.
            value(int): value to fill gaps; if -1 fill values using 'backfill' mode.
        Returns:
            pandas.Series: New feature generated.
        """
        if self._fillna:
            series_output = series.copy(deep=False)
            series_output = series_output.replace([np.inf, -np.inf], np.nan)
            if isinstance(value, int) and value == -1:
                series = series_output.fillna(method="ffill").fillna(value=-1)
            else:
                series = series_output.fillna(method="ffill").fillna(value)
        return series

    @staticmethod
    def _true_range(
        high: pd.Series, low: pd.Series, prev_close: pd.Series
    ) -> pd.Series:
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        true_range = pd.DataFrame(data={"tr1": tr1, "tr2": tr2, "tr3": tr3}).max(axis=1)
        return true_range


def dropna(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows with "Nans" values"""
    df = df.copy()
    number_cols = df.select_dtypes("number").columns.to_list()
    df[number_cols] = df[number_cols][df[number_cols] < math.exp(709)]  # big number
    df[number_cols] = df[number_cols][df[number_cols] != 0.0]
    df = df.dropna()
    return df


def _sma(series, periods: int, fillna: bool = False):
    min_periods = 0 if fillna else periods
    return series.rolling(window=periods, min_periods=min_periods).mean()


def _ema(series, periods, fillna=False):
    min_periods = 0 if fillna else periods
    return series.ewm(span=periods, min_periods=min_periods, adjust=False).mean()


def _get_min_max(series1: pd.Series, series2: pd.Series, function: str = "min"):
    """Find min or max value between two lists for each index"""
    series1 = np.array(series1)
    series2 = np.array(series2)
    if function == "min":
        output = np.amin([series1, series2], axis=0)
    elif function == "max":
        output = np.amax([series1, series2], axis=0)
    else:
        raise ValueError('"f" variable value should be "min" or "max"')

    return pd.Series(output)


class DonchianChannel(IndicatorMixin):
    """Donchian Channel
    https://www.investopedia.com/terms/d/donchianchannels.asp
    Args:
        high(pandas.Series): dataset 'High' column.
        low(pandas.Series): dataset 'Low' column.
        close(pandas.Series): dataset 'Close' column.
        window(int): n period.
        fillna(bool): if True, fill nan values.
    """

    def __init__(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        window: int = 20,
        offset: int = 0,
        fillna: bool = False,
    ):
        self._offset = offset
        self._close = close
        self._high = high
        self._low = low
        self._window = window
        self._fillna = fillna
        self._run()

    def _run(self):
        self._min_periods = 1 if self._fillna else self._window
        self._hband = self._high.rolling(
            self._window, min_periods=self._min_periods
        ).max()
        self._lband = self._low.rolling(
            self._window, min_periods=self._min_periods
        ).min()

    def donchian_channel_hband(self) -> pd.Series:
        """Donchian Channel High Band
        Returns:
            pandas.Series: New feature generated.
        """
        hband = self._check_fillna(self._hband, value=-1)
        if self._offset != 0:
            hband = hband.shift(self._offset)
        return pd.Series(hband, name="dchband")

    def donchian_channel_lband(self) -> pd.Series:
        """Donchian Channel Low Band
        Returns:
            pandas.Series: New feature generated.
        """
        lband = self._check_fillna(self._lband, value=-1)
        if self._offset != 0:
            lband = lband.shift(self._offset)
        return pd.Series(lband, name="dclband")

    def donchian_channel_mband(self) -> pd.Series:
        """Donchian Channel Middle Band
        Returns:
            pandas.Series: New feature generated.
        """
        mband = ((self._hband - self._lband) / 2.0) + self._lband
        mband = self._check_fillna(mband, value=-1)
        if self._offset != 0:
            mband = mband.shift(self._offset)
        return pd.Series(mband, name="dcmband")

    def donchian_channel_wband(self) -> pd.Series:
        """Donchian Channel Band Width
        Returns:
            pandas.Series: New feature generated.
        """
        mavg = self._close.rolling(self._window, min_periods=self._min_periods).mean()
        wband = ((self._hband - self._lband) / mavg) * 100
        wband = self._check_fillna(wband, value=0)
        if self._offset != 0:
            wband = wband.shift(self._offset)
        return pd.Series(wband, name="dcwband")

    def donchian_channel_pband(self) -> pd.Series:
        """Donchian Channel Percentage Band
        Returns:
            pandas.Series: New feature generated.
        """
        pband = (self._close - self._lband) / (self._hband - self._lband)
        pband = self._check_fillna(pband, value=0)
        if self._offset != 0:
            pband = pband.shift(self._offset)
        return pd.Series(pband, name="dcpband")


class RSIIndicator(IndicatorMixin):
    """Relative Strength Index (RSI)
    Compares the magnitude of recent gains and losses over a specified time
    period to measure speed and change of price movements of a security. It is
    primarily used to attempt to identify overbought or oversold conditions in
    the trading of an asset.
    https://www.investopedia.com/terms/r/rsi.asp
    Args:
        close(pandas.Series): dataset 'Close' column.
        window(int): n period.
        fillna(bool): if True, fill nan values.
    """

    def __init__(self, close: pd.Series, window: int = 14, fillna: bool = False):
        self._close = close
        self._window = window
        self._fillna = fillna
        self._run()

    def _run(self):
        diff = self._close.diff(1)
        up_direction = diff.where(diff > 0, 0.0)
        down_direction = -diff.where(diff < 0, 0.0)
        min_periods = 0 if self._fillna else self._window
        emaup = up_direction.ewm(
            alpha=1 / self._window, min_periods=min_periods, adjust=False
        ).mean()
        emadn = down_direction.ewm(
            alpha=1 / self._window, min_periods=min_periods, adjust=False
        ).mean()
        relative_strength = emaup / emadn
        self._rsi = pd.Series(
            np.where(emadn == 0, 100, 100 - (100 / (1 + relative_strength))),
            index=self._close.index,
        )

    def rsi(self) -> pd.Series:
        """Relative Strength Index (RSI)
        Returns:
            pandas.Series: New feature generated.
        """
        rsi_series = self._check_fillna(self._rsi, value=50)
        return pd.Series(rsi_series, name="rsi")


class KAMAIndicator(IndicatorMixin):
    """Kaufman's Adaptive Moving Average (KAMA)
    Moving average designed to account for market noise or volatility. KAMA
    will closely follow prices when the price swings are relatively small and
    the noise is low. KAMA will adjust when the price swings widen and follow
    prices from a greater distance. This trend-following indicator can be
    used to identify the overall trend, time turning points and filter price
    movements.
    https://www.tradingview.com/ideas/kama/
    Args:
        close(pandas.Series): dataset 'Close' column.
        window(int): n period.
        pow1(int): number of periods for the fastest EMA constant.
        pow2(int): number of periods for the slowest EMA constant.
        fillna(bool): if True, fill nan values.
    """

    def __init__(
        self,
        close: pd.Series,
        window: int = 10,
        pow1: int = 2,
        pow2: int = 30,
        fillna: bool = False,
    ):
        self._close = close
        self._window = window
        self._pow1 = pow1
        self._pow2 = pow2
        self._fillna = fillna
        self._run()

    def _run(self):
        close_values = self._close.values
        vol = pd.Series(abs(self._close - np.roll(self._close, 1)))

        min_periods = 0 if self._fillna else self._window
        er_num = abs(close_values - np.roll(close_values, self._window))
        er_den = vol.rolling(self._window, min_periods=min_periods).sum()
        efficiency_ratio = er_num / er_den

        smoothing_constant = (
            (
                efficiency_ratio * (2.0 / (self._pow1 + 1) - 2.0 / (self._pow2 + 1.0))
                + 2 / (self._pow2 + 1.0)
            )
            ** 2.0
        ).values

        self._kama = np.zeros(smoothing_constant.size)
        len_kama = len(self._kama)
        first_value = True

        for i in range(len_kama):
            if np.isnan(smoothing_constant[i]):
                self._kama[i] = np.nan
            elif first_value:
                self._kama[i] = close_values[i]
                first_value = False
            else:
                self._kama[i] = self._kama[i - 1] + smoothing_constant[i] * (
                    close_values[i] - self._kama[i - 1]
                )

    def kama(self) -> pd.Series:
        """Kaufman's Adaptive Moving Average (KAMA)
        Returns:
            pandas.Series: New feature generated.
        """
        kama_series = pd.Series(self._kama, index=self._close.index)
        kama_series = self._check_fillna(kama_series, value=self._close)
        return pd.Series(kama_series, name="kama")


class AverageTrueRange(IndicatorMixin):
    """Average True Range (ATR)
    The indicator provide an indication of the degree of price volatility.
    Strong moves, in either direction, are often accompanied by large ranges,
    or large True Ranges.
    http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:average_true_range_atr
    Args:
        high(pandas.Series): dataset 'High' column.
        low(pandas.Series): dataset 'Low' column.
        close(pandas.Series): dataset 'Close' column.
        window(int): n period.
        fillna(bool): if True, fill nan values.
    """

    def __init__(
        self,
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        window: int = 14,
        fillna: bool = False,
    ):
        self._high = high
        self._low = low
        self._close = close
        self._window = window
        self._fillna = fillna
        self._run()

    def _run(self):
        close_shift = self._close.shift(1)
        true_range = self._true_range(self._high, self._low, close_shift)
        atr = np.zeros(len(self._close))
        atr[self._window - 1] = true_range[0 : self._window].mean()
        for i in range(self._window, len(atr)):
            atr[i] = (atr[i - 1] * (self._window - 1) + true_range.iloc[i]) / float(
                self._window
            )
        self._atr = pd.Series(data=atr, index=true_range.index)

    def average_true_range(self) -> pd.Series:
        """Average True Range (ATR)
        Returns:
            pandas.Series: New feature generated.
        """
        atr = self._check_fillna(self._atr, value=0)
        return pd.Series(atr, name="atr")


class BollingerBands(IndicatorMixin):
    """Bollinger Bands
    https://school.stockcharts.com/doku.php?id=technical_indicators:bollinger_bands
    Args:
        close(pandas.Series): dataset 'Close' column.
        window(int): n period.
        window_dev(int): n factor standard deviation
        fillna(bool): if True, fill nan values.
    """

    def __init__(
        self,
        close: pd.Series,
        window: int = 20,
        window_dev: int = 2,
        fillna: bool = False,
    ):
        self._close = close
        self._window = window
        self._window_dev = window_dev
        self._fillna = fillna
        self._run()

    def _run(self):
        min_periods = 0 if self._fillna else self._window
        self._mavg = self._close.rolling(self._window, min_periods=min_periods).mean()
        self._mstd = self._close.rolling(self._window, min_periods=min_periods).std(
            ddof=0
        )
        self._hband = self._mavg + self._window_dev * self._mstd
        self._lband = self._mavg - self._window_dev * self._mstd

    def bollinger_mavg(self) -> pd.Series:
        """Bollinger Channel Middle Band
        Returns:
            pandas.Series: New feature generated.
        """
        mavg = self._check_fillna(self._mavg, value=-1)
        return pd.Series(mavg, name="mavg")

    def bollinger_hband(self) -> pd.Series:
        """Bollinger Channel High Band
        Returns:
            pandas.Series: New feature generated.
        """
        hband = self._check_fillna(self._hband, value=-1)
        return pd.Series(hband, name="hband")

    def bollinger_lband(self) -> pd.Series:
        """Bollinger Channel Low Band
        Returns:
            pandas.Series: New feature generated.
        """
        lband = self._check_fillna(self._lband, value=-1)
        return pd.Series(lband, name="lband")

    def bollinger_wband(self) -> pd.Series:
        """Bollinger Channel Band Width
        From: https://school.stockcharts.com/doku.php?id=technical_indicators:bollinger_band_width
        Returns:
            pandas.Series: New feature generated.
        """
        wband = ((self._hband - self._lband) / self._mavg) * 100
        wband = self._check_fillna(wband, value=0)
        return pd.Series(wband, name="bbiwband")

    def bollinger_pband(self) -> pd.Series:
        """Bollinger Channel Percentage Band
        From: https://school.stockcharts.com/doku.php?id=technical_indicators:bollinger_band_perce
        Returns:
            pandas.Series: New feature generated.
        """
        pband = (self._close - self._lband) / (self._hband - self._lband)
        pband = self._check_fillna(pband, value=0)
        return pd.Series(pband, name="bbipband")

    def bollinger_hband_indicator(self) -> pd.Series:
        """Bollinger Channel Indicator Crossing High Band (binary).
        It returns 1, if close is higher than bollinger_hband. Else, it returns 0.
        Returns:
            pandas.Series: New feature generated.
        """
        hband = pd.Series(
            np.where(self._close > self._hband, 1.0, 0.0), index=self._close.index
        )
        hband = self._check_fillna(hband, value=0)
        return pd.Series(hband, index=self._close.index, name="bbihband")

    def bollinger_lband_indicator(self) -> pd.Series:
        """Bollinger Channel Indicator Crossing Low Band (binary).
        It returns 1, if close is lower than bollinger_lband. Else, it returns 0.
        Returns:
            pandas.Series: New feature generated.
        """
        lband = pd.Series(
            np.where(self._close < self._lband, 1.0, 0.0), index=self._close.index
        )
        lband = self._check_fillna(lband, value=0)
        return pd.Series(lband, name="bbilband")
