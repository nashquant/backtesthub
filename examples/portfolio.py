#! /usr/bin/env python3

import os, sys
import pandas as pd
from typing import Sequence
from collections import defaultdict
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.append(
    os.path.dirname(
        os.path.dirname(__file__),
    )
)

from backtesthub.indicators import *
from backtesthub.strategy import Strategy
from backtesthub.pipelines import Portfolio
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.config import (
    _DEFAULT_URL,
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_CURRENCY,
)

pd.options.mode.chained_assignment = None

######################### CONFIG #########################

factor = "PORTFOLIO"
market = "PORTFOLIO"
asset = "PORTFOLIO"
oc = ["open", "close"]

config = {
    "factor": factor,
    "market": market,
    "asset": asset,
}

##########################################################
##################  DATABASE OPERATIONS ##################

engine = create_engine(
    URL.create(**_DEFAULT_URL),
    pool_pre_ping=True,
    echo=False,
)

meta_sql = (
    "SELECT h.uid, h.version, h.value as allocation, s.fromdate, s.todate "
    "FROM quant.Hierarchy h INNER JOIN quant.Strategies s "
    "ON s.uid = h.uid"
)

price_sql = (
    "SELECT date, uid, open, close "
    "FROM quant.Quotas WHERE uid in "
    "(SELECT uid FROM quant.Hierarchy)"
)

meta = pd.read_sql(meta_sql, engine)
price = pd.read_sql(price_sql, engine)

meta.set_index("uid", inplace=True)
price.set_index("date", inplace=True)

key, val = meta.version, meta.allocation
hierarchy = dict(zip(tuple(key), tuple(val)))

##########################################################
##################### STRATEGY SETUP #####################


class Hierarchy(Strategy):

    """
    `Hierarchy Strategy Class`

    This class extends from Base Strategy Class.

    It implements the HRP mentioned at Portfolio Pipeline,
    which basically just takes some external input regarding
    the static risk budget allocation, and make a logic to
    maintain the allocation within the range given a 5% threshold.

    IMPORTANT: It assumes that all underlying strategies were back-
    tested using the same _DEFAULT_VOLATILITY (refer to ~/backtest/
    utils/config.py), otherwise allocation process will be WRONG!

    """

    from backtesthub.utils.config import (
        _DEFAULT_VOLATILITY,
        _DEFAULT_PORTFVOLAT,
    )

    params = {
        "vol_strat": _DEFAULT_VOLATILITY,
        "vol_portf": _DEFAULT_PORTFVOLAT,
    }

    def init(self):
        for asset in self.assets.values():
            self.I(
                data=asset,
                func=Buy_n_Hold,
                name="signal",
                **self.params,
            )

            self.V(
                data=asset,
            )

        self.vol_strat = self.params["vol_strat"]
        self.vol_portf = self.params["vol_portf"]
        self.scale = self.vol_portf / self.vol_strat

    def next(self):
        univ = self.get_universe()

        for asset in univ:
            alloc = hierarchy[asset.ticker]
            budget = self.scale * alloc
            self.order_target(
                data=asset,
                target=budget,
            )


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
    strategy=Hierarchy,
    pipeline=Portfolio,
    calendar=calendar,
    **config,
)

for uid, row in meta.iterrows():
    mask = price.uid == uid
    data = price[mask]

    commkwargs = dict(
        multiplier=1,
        currency=_DEFAULT_CURRENCY,
        inception=row["fromdate"],
        maturity=row["todate"],
    )

    backtest.add_asset(
        ticker=row["version"],
        data=data[oc],
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

