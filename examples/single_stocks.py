#! /usr/bin/env python3
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv

file_dir = os.path.dirname(__file__)
base_dir = os.path.dirname(file_dir)

sys.path.append(base_dir)
load_dotenv()

from backtesthub.indicators.indicator import (
    Buy_n_Hold,
)
from backtesthub.pipelines.pipeline import (
    Single,
)
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.bases import Line
from backtesthub.utils.math import adjust_stocks
from backtesthub.utils.config import (
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_URL,
)

pd.options.mode.chained_assignment = None

######################### CONFIG #########################

base = "IMAB5+"
factor = "RISKPAR"
market = "RATESBR"
asset = "IB5M11"
ohlc = ["open", "high", "low", "close"]
ohlcr = ["open", "high", "low", "close", "returns"]

config = {
    "factor": factor,
    "market": market,
    "asset": asset,
    "base": base,
}

##########################################################
##################### STRATEGY SETUP #####################


class Riskpar_BuyNHold(Strategy):
    params = {}

    def init(self):
        signal = self.I(
            data=self.base,
            func=Buy_n_Hold,
            **self.params,
        )

        volatility = self.V(
            data=self.base,
        )

        self.base.add_line(
            name="signal",
            line=Line(array=signal),
        )
        
        self.base.add_line(
            name="volatility",
            line=Line(array=volatility),
        )

        self.broadcast(
            base=self.base,
            assets=self.assets,
            lines=["signal", "volatility"],
        )

    def next(self):
        univ = self.get_universe()

        for asset in univ:
            self.order_target(
                data=asset,
                target=self.sizing(
                    data=asset,
                ),
            )


##########################################################
##################  DATABASE OPERATIONS ##################

engine = create_engine(
    URL.create(**_DEFAULT_URL),
    pool_pre_ping=True,
    echo=False,
)

base_hist = os.getenv("BASE_HIST")
stocks_hist = os.getenv("STK_HIST")

base_sql = (
    "SELECT date, ticker, open, high, low, close "
    f"FROM {base_hist} "
    f"WHERE ticker = '{base}' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

carry_sql = (
    "SELECT date, open, high, low, close "
    f"FROM {base_hist} "
    f"WHERE ticker = 'CARRY' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close, returns/100 as returns "
    f"FROM {stocks_hist} s "
    f"WHERE s.ticker = '{asset}' AND "
    f"date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price = pd.read_sql(price_sql, engine)
b_price = pd.read_sql(base_sql, engine)
carry = pd.read_sql(carry_sql, engine)

price.set_index("date", inplace=True)
b_price.set_index("date", inplace=True)
carry.set_index("date", inplace=True)

carry = carry.pct_change()

##########################################################
####################  MAIN OPERATIONS ####################

calendar = Calendar(
    start=_DEFAULT_SDATE,
    end=min(
        _DEFAULT_EDATE,
        max(price.index),
    ),
    country="BR",
)

backtest = Backtest(
    strategy=Riskpar_BuyNHold,
    pipeline=Single,
    calendar=calendar,
    **config,
)

backtest.add_base(
    ticker=base,
    data=b_price[ohlc],
)

backtest.add_base(
    ticker="carry",
    data=carry[ohlc],
)


backtest.add_asset(
    ticker=asset,
    data=adjust_stocks(
        price[ohlcr],
    ),
)

res = backtest.run()

##########################################################
################### RESULTS MANAGEMENT ###################

strat, strat_meta = res["meta"], res["meta"].iloc[0, :]
df, rec = res["quotas"], res["records"]

strat.set_index("uid", inplace=True)
df.set_index("date", inplace=True)
rec.set_index("date", inplace=True)
