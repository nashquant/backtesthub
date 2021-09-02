#! /usr/bin/env python3
import os, sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.append(
    os.path.dirname(
        os.path.dirname(__file__),
    )
)

from backtesthub.indicators import *
from backtesthub.pipelines import Single
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.math import adjust_stocks
from backtesthub.utils.config import (
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_URL,
)

pd.options.mode.chained_assignment = None

################## CONFIG ##################

base = "IMAB5+"
obases = ["CARRY"]
factor = "RISKPAR"
market = "RATESBR"
asset = "IB5M11"
ohlc = ["open", "high", "low", "close"]
ohlcr = ["open", "high", "low", "close", "returns"]

############################################


class Riskpar(Strategy):

    params = {}

    def init(self):
        self.I(
            self.base,
            Buy_n_Hold,
            **self.params,
        )

        self.broadcast(
            self.base,
            self.assets,
        )

    def next(self):
        for asset in self.get_universe():
            self.order_target(asset)


calendar = Calendar(
    start=_DEFAULT_SDATE,
    end=_DEFAULT_EDATE,
    country="BR",
)

backtest = Backtest(
    strategy=Riskpar,
    pipeline=Single,
    calendar=calendar,
    factor=factor,
    market=market,
    asset=asset,
)

engine = create_engine(
    URL.create(**_DEFAULT_URL),
    pool_pre_ping=True,
    echo=False,
)

##################  DATABASE OPERATIONS ##################

base_sql = (
    "SELECT date, ticker, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker = '{base}' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

carry_sql = (
    "SELECT date, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker = 'CARRY' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close, returns/100 as returns "
    "FROM quant.StocksHistory s "
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
strat_meta = res["meta"]
df, rec = res["quotas"], res["records"]

pd.options.display.float_format = "{:,.2f}".format

df["volatility"] = df.volatility * 100
df["drawdown"] = df.drawdown * 100

print("\n" + str(res))

cols = [
    "sharpe",
    "volatility",
    "drawdown",
]

print(df[cols].mean())
