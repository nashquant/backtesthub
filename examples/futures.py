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
from backtesthub.pipelines import Rolling
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.config import (
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_URL,
)

pd.options.mode.chained_assignment = None

class System(Strategy):

    p1 = 10
    p2 = 60

    def init(self):
        self.I(
            self.base,
            SMACross,
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
    pipeline=Rolling,
    calendar=calendar,
)

engine = create_engine(
    URL.create(**_DEFAULT_URL),
    pool_pre_ping=True,
    echo=False,
)

##### LOYALL DATABASE OPERATIONS #####

base = "T10"
obases = ["USDBRL"]
commodity = "T10"
ohlc = ["open", "high", "low", "close"]

base_sql = (
    "SELECT date, ticker, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker = '{base}' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

obase_sql = (
    "SELECT date, ticker, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker IN ({str(obases)[1:-1]}) AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

meta_sql = (
    "SELECT f.ticker as ticker, c.currency as curr, c.multiplier as mult, "
    "f.endDate as mat FROM quant.Commodities c "
    "INNER JOIN quant.Futures f ON c.ticker = f.commodity "
    f"WHERE c.ticker IN ('{commodity}') AND f.endDate > '{_DEFAULT_SDATE}' "
    "ORDER BY mat"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close "
    "FROM quant.FuturesHistory f "
    f"WHERE f.commodity = '{commodity}' AND "
    f"date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

meta = pd.read_sql(meta_sql, engine)
price = pd.read_sql(price_sql, engine)
b_price = pd.read_sql(base_sql, engine)
ob_price = pd.read_sql(obase_sql, engine)

meta.set_index("ticker", inplace=True)
price.set_index("date", inplace=True)
b_price.set_index("date", inplace=True)
ob_price.set_index("date", inplace=True)

########################################

backtest.add_base(
    ticker=base,
    data=b_price[ohlc],
)

for obase in obases:
    mask = ob_price.ticker == obase
    data = ob_price[mask]
    backtest.add_base(
        ticker=obase,
        data=data[ohlc],
    )

for ticker, prop in meta.iterrows():
    mask = price.ticker == ticker
    data = price[mask]

    commkwargs = dict(
        multiplier=prop.mult,
        currency=prop.curr,
        maturity=prop.mat,
    )

    backtest.add_asset(
        ticker=ticker,
        data=data[ohlc],
        **commkwargs,
    )

res = backtest.run()
df, rec = res.df, res.rec

pd.options.display.float_format = "{:,.2f}".format

df['volatility'] = df.volatility * 100
df['drawdown'] = df.drawdown * 100

print("\n" + str(res))

cols = [
    "sharpe",
    "volatility",
    "drawdown",
]

print(df[cols].mean())
