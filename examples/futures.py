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

######################### CONFIG #########################

base = "USDBRL"
obases = ["CARRY"]
factor = "TREND"
market = "FXBR"
asset = "DOL"
ohlc = ["open", "high", "low", "close"]

config = {
    "factor": factor,
    "market": market,
    "asset": asset,
    "base": base,
}

##########################################################
##################### STRATEGY SETUP #####################

class Trend_SMACross(Strategy):

    params = {
        "p1": 10,
        "p2": 200,
    }

    def init(self):
        self.I(
            data=self.base,
            func=SMACross,
            name="signal",
            **self.params,
        )

        self.V(
            data=self.base,
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
    f"WHERE c.ticker IN ('{asset}') AND f.endDate > '{_DEFAULT_SDATE}' "
    "ORDER BY mat"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close "
    "FROM quant.FuturesHistory f "
    f"WHERE f.commodity = '{asset}' AND "
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
    pipeline=Rolling,
    calendar=calendar,
    **config
)

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

##########################################################
################### RESULTS MANAGEMENT ###################

strat, strat_meta = res["meta"], res["meta"].iloc[0, :]
df, rec = res["quotas"], res["records"]

strat.set_index("uid", inplace=True)
df.set_index("date", inplace=True)
rec.set_index("date", inplace=True)

##########################################################
##################### RESULTS OUTPUT #####################

with engine.connect().execution_options(autocommit=True) as conn:
    conn.execute(f"DELETE FROM quant._Strategies WHERE uid IN ('{strat_meta['uid']}') ")
    conn.execute(f"DELETE FROM quant._Quotas WHERE uid IN ('{strat_meta['uid']}')")
    conn.execute(f"DELETE FROM quant._Positions WHERE uid IN ('{strat_meta['uid']}')")

strat.to_sql(
    "_Strategies",
    con=engine,
    if_exists="append",
)

df.to_sql(
    "_Quotas",
    con=engine,
    if_exists="append",
)

rec.to_sql(
    "_Positions",
    con=engine,
    if_exists="append",
)
