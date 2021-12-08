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
    Vertice,
)
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.bases import Line
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
        "p2": 100,
    }

    def init(self):
        for ticker in self.assets:
            
            ## Upward move in rates,
            ## implies downward move
            ## in price -> revert sig.
            
            signal = -self.I(
                data=self.bases[ticker],
                func=SMACross,
                **self.params,
            )

            volatility = self.V(
                data=self.assets[ticker],
            )

            self.bases[ticker].add_line(
                name="signal",
                line=Line(array=signal),
            )
            
            self.assets[ticker].add_line(
                name="volatility",
                line=Line(array=volatility),
            )

            self.broadcast(
                base=self.bases[ticker],
                assets={ticker: self.assets[ticker]},
                lines=["signal"],
            )

    def next(self):
        chain = self.get_chain()
        self.universe = [chain[-v] for v in vertices]

        for asset in self.universe:
            self.order_target(
                data=asset,
                target=self.sizing(
                    data=asset,
                )/len(self.universe),
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

carry_sql = (
    f"SELECT date, open, high, low, close FROM {base_hist} "
    f"WHERE ticker = 'CARRY' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

meta_sql = (
    "SELECT f.ticker as ticker, c.currency as curr, c.multiplier as mult, "
    f"f.endDate as mat FROM {comm_meta} c "
    f"INNER JOIN {fut_meta} f ON c.ticker = f.commodity "
    f"WHERE c.ticker IN ('{asset}') AND f.ticker LIKE '%%{tenor}%%' "
    f"AND f.endDate > '{_DEFAULT_SDATE}'"
    "ORDER BY mat"
)

price_sql = (
    "SELECT ticker, date, open, high, low, close "
    f"FROM {fut_hist} f "
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

strat, strat_meta = res["meta"], res["meta"].iloc[0, :]
df, rec = res["quotas"], res["records"]

strat.set_index("uid", inplace=True)
df.set_index("date", inplace=True)
rec.set_index("date", inplace=True)
