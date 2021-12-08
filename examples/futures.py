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
    SMACross,
)
from backtesthub.pipelines.pipeline import (
    Rolling,
)
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.bases import Line
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
        "p2": 100,
    }

    def init(self):
        signal = self.I(
            data=self.base,
            func=SMACross,
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
comm_meta = os.getenv("COMM_META")
fut_meta = os.getenv("FUT_META")
fut_hist = os.getenv("FUT_HIST")

base_sql = (
    f"SELECT date, ticker, open, high, low, close FROM {base_hist} "
    f"WHERE ticker = '{base}' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

obase_sql = (
    f"SELECT date, ticker, open, high, low, close FROM {base_hist} "
    f"WHERE ticker IN ({str(obases)[1:-1]}) AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

meta_sql = (
    f"SELECT f.ticker as ticker, c.currency as curr, c.multiplier as mult, "
    f"f.endDate as mat FROM {comm_meta} c "
    f"INNER JOIN {fut_meta} f ON c.ticker = f.commodity "
    f"WHERE c.ticker IN ('{asset}') AND f.endDate > '{_DEFAULT_SDATE}' "
    "ORDER BY mat"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close "
    f"FROM {fut_hist} f "
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
