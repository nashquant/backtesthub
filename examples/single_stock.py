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

from backtesthub.indicators import Default, SMACross
from backtesthub.pipelines import Single
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.math import fill_OHLC
from backtesthub.utils.config import (
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_URL,
)


class System(Strategy):

    p1 = 10
    p2 = 200

    def init(self):
        self.I(
            self.base,
            Default,
            self.p1,
            self.p2,
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
    strategy=System,
    pipeline=Single,
    calendar=calendar,
)

engine = create_engine(
    URL.create(**_DEFAULT_URL),
    pool_pre_ping=True,
    echo=False,
)

##### LOYALL DATABASE OPERATIONS #####

base = "IMAB5+"
obases= ["CARRY"]
asset = "IB5M11"
ohlc = ["open", "high", "low", "close"]

base_sql = (
    "SELECT date, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker = '{base}' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

obase_sql = (
    "SELECT date, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker IN ({str(obases)[1:-1]}) AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close "
    "FROM quant.StocksHistory s "
    f"WHERE s.ticker = '{asset}' AND "
    f"date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price = pd.read_sql(price_sql, engine)
b_price = pd.read_sql(base_sql, engine)
ob_price = pd.read_sql(obase_sql, engine)

price.set_index("date", inplace=True)
b_price.set_index("date", inplace=True)
ob_price.set_index("date", inplace=True)

########################################

backtest.add_base(
    ticker=base,
    data=b_price[ohlc],
)

for obase in obases:

    backtest.add_base(
        ticker=obase,
        data=ob_price[ohlc],
    )


backtest.add_asset(
    ticker=asset,
    data=price[ohlc],
)

res = backtest.run()
df, rec = res.df, res.rec

pd.options.display.float_format = "{:,.2f}".format

df['volatility'] = 100 * df.volatility
df['drawdown'] = 100 * df.drawdown

print("\n" + str(res))

cols = [
    "sharpe",
    "volatility",
    "drawdown",
]

print(df[cols].mean())
