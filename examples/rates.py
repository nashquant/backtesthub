#! /usr/bin/env python3

import os, sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv

load_dotenv()

sys.path.append(
    os.path.dirname(
        os.path.dirname(__file__),
    )
)

from backtesthub.indicators.indicator import (
    RevSMACross,
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
            signal = self.I(
                data=self.bases[ticker],
                func=RevSMACross,
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
