#! /usr/bin/env python3

import numpy as np
import pandas as pd

from numbers import Number
from warnings import filterwarnings
from typing import List, Dict, Optional

from .broker import Broker
from .strategy import Strategy
from .utils.config import _SCHEMA
from .utils.types import Data, Line


filterwarnings("ignore")


class Backtest:
    """
    Backtest a parameterized strategy
    on single/multi data, hedged/unhedged
    data or even stock/futures data.

    """

    def __init__(
        self,
        datas: Dict[str, pd.DataFrame],
        strategy: Optional[Strategy],
        cash: float = float("10e6"),
    ):

        """
        * Initialize a backtest. Requires data and a strategy to test.

        * `datas` is a sequence of `pd.DataFrame` with columns:
          `Open`, `High`, `Low`, `Close`, (optionally) `Volume`.

          The passed data frame can contain additional columns that
          can be used by the strategy.

          DataFrame index can be either a datetime index (timestamps)
          or a monotonic range index (i.e. a sequence of periods).

        * `strategy` is a `backtesting.backtesting.Strategy`
          _subclass_ (not an instance).

        * `cash` is the initial cash to start with.

        """

        ## SET VARIABLES ##

        self.__datas = datas
        self.__strategy = strategy
        self.__cash = cash

        self.__ohlc: List[str] = _SCHEMA["OHLC"]
        self.__ohlcv: List[str] = _SCHEMA["OHLCV"]

        ## VERIFY INTEGRITY ##

        if not (issubclass(self.__strategy, Strategy)):
            msg = "Arg `strategy` must be a Strategy sub-type"
            raise TypeError(msg)

        for data in self.__datas:
            if not isinstance(data, pd.DataFrame):
                msg = "Arg `data` must be a pandas.DataFrame"
                raise TypeError()

            if len(data) == 0:
                msg = "OHLC `data` is empty"
                raise ValueError()

            rename = {c: c.lower() for c in data.columns}
            data.rename(columns=rename, inplace=True)

            if len(data.columns.intersection(set(self.__ohlc))) < 4:
                msg = "Arg `datas` must hold OHLC schemed pandas.DataFrame"
                raise ValueError(msg)

        ## BUILD ENTITIES ##

        self.__broker = Broker(
            datas=self.__datas,
            cash=self.__cash,
        )

    def run(self) -> Dict[str, Number]:

        broker = self.__broker
        strategy = self.__strategy

        strategy.init()

        # Indicators used in Strategy.next()
        indicator_attrs = {
            attr: indicator
            for attr, indicator in strategy.__dict__.items()
            if isinstance(indicator, Line)
        }.items()

        start_buffer = 1 + max(
            (
                np.isnan(indicator.astype(float)).argmin(axis=-1).max()
                for _, indicator in indicator_attrs
            ),
            default=0,
        )

        with np.errstate(invalid="ignore"):

            for i in range(start_buffer, len(self.__index)):

                for data in self.__datas:

                    data._set_buffer(i + 1)

                    for attr, indicator in indicator_attrs:
                        setattr(strategy, attr, indicator[..., : i + 1])

                    broker.next()

                    # Next tick, a moment before bar close
                    strategy.next()
                else:
                    # Close any remaining open trades so they produce some stats
                    for trade in broker.trades:
                        trade.close()

                    # Re-run broker one last time to handle orders placed in the last strategy
                    # iteration. Use the same OHLC values as in the last broker iteration.
                    if start_buffer < len(self._data):
                        broker.next
