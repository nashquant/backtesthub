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
        self.I(
            data=self.base,
            func=Buy_n_Hold,
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
