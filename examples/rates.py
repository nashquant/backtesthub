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
from backtesthub.pipelines import Vertice
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.math import fill_OHLC, rate2price
from backtesthub.utils.config import (
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_URL,
)

pd.options.mode.chained_assignment = None

######################### CONFIG #########################

obases = ["CARRY"]
factor = "TREND"
market = "RATESBR"
asset = "DI1"
vertices, tenor = [1, 2, 3], "F"
ohlc = ["open", "high", "low", "close"]

config = {
    "factor": factor,
    "market": market,
    "asset": asset,
    "vertices": vertices,
}

##########################################################
##################### STRATEGY SETUP #####################

class Trend_SMACross(Strategy):

    params = {
        "p1": 10,
        "p2": 200,
    }

    def init(self):
        for ticker in self.assets:
            self.I(
                data=self.bases[ticker],
                func=RevSMACross,
                name="signal",
                **self.params,
            )

            self.broadcast(
                base=self.bases[ticker],
                assets={ticker: self.assets[ticker]},
                lines=["signal"],
            )

            self.V(
                data=self.assets[ticker],
            )

    def next(self):
        chain = self.get_chain()
        self.universe = [chain[-v] for v in vertices]

        for asset in self.universe:
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

carry_sql = (
    "SELECT date, open, high, low, close FROM quant.IndexesHistory "
    f"WHERE ticker = 'CARRY' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

meta_sql = (
    "SELECT f.ticker as ticker, c.currency as curr, c.multiplier as mult, "
    "f.endDate as mat FROM quant.Commodities c "
    "INNER JOIN quant.Futures f ON c.ticker = f.commodity "
    f"WHERE c.ticker IN ('{asset}') AND f.ticker LIKE '%%{tenor}%%' "
    f"AND f.endDate > '{_DEFAULT_SDATE}'"
    "ORDER BY mat"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close "
    "FROM quant.FuturesHistory f "
    f"WHERE f.commodity = '{asset}' AND f.ticker LIKE '%%{tenor}%%' "
    f"AND date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

meta = pd.read_sql(meta_sql, engine)
price = pd.read_sql(price_sql, engine)
carry = pd.read_sql(carry_sql, engine)

meta.set_index("ticker", inplace=True)
price.set_index("date", inplace=True)
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
    strategy=Trend_SMACross,
    pipeline=Vertice,
    calendar=calendar,
    **config,
)


backtest.add_base(
    ticker="carry",
    data=carry[ohlc],
)

for ticker, prop in meta.iterrows():
    mask = price.ticker == ticker
    data = price[mask]
    data = fill_OHLC(data)

    commkwargs = dict(
        multiplier=prop.mult,
        currency=prop.curr,
        maturity=prop.mat,
    )

    backtest.add_base(
        ticker=ticker,
        data=data[ohlc],
    )

    backtest.add_asset(
        ticker=ticker,
        data=rate2price(
            data=data[ohlc],
            maturity=prop.mat,
        ),
        **commkwargs,
    )

res = backtest.run()

##########################################################
################### RESULTS MANAGEMENT ###################

strat_meta = res["meta"].iloc[0, :]
df, rec = res["quotas"], res["records"]

print("\n" + str(strat_meta))
print("\n" + str(df))
print("\n" + str(rec))
