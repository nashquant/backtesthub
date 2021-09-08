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
from backtesthub.pipelines import VA_Ranking
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

factor = "ALTBETA"
market = "STOCKSBR@MOM"
asset = "MULTI"
volume = "VOBZBRC"
ohlc = ["open", "high", "low", "close"]
ohlcr = ["open", "high", "low", "close", "returns"]
ohlcv = ["open", "high", "low", "close", "liquidity"]
ohlcrv = ["open", "high", "low", "close", "returns", "liquidity"]

config = {
    "factor": factor,
    "market": market,
    "asset": asset,
}

##########################################################
##################### STRATEGY SETUP #####################


class Trend_SMARatio(Strategy):
    params = {
        "p1": 10,
        "p2": 200,
    }

    def init(self):

        for asset in self.assets:
            self.I(
                data=self.assets[asset],
                func=SMARatio,
                name="indicator",
                **self.params,
            )

            self.I(
                data=self.assets[asset],
                func=Buy_n_Hold,
                name="signal",
                **self.params,
            )

            self.V(
                data=self.assets[asset],
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


carry_sql = (
    "SELECT date, open, high, low, close "
    "FROM quant.IndexesHistory "
    f"WHERE ticker = 'CARRY' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price_sql = (
    "SELECT date, ticker, open, high, low, close, "
    "returns, liquidity "
    "FROM MultiStocksHistory "
    f"WHERE date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price = pd.read_sql(price_sql, engine)
carry = pd.read_sql(carry_sql, engine)

price.set_index("date", inplace=True)
carry.set_index("date", inplace=True)

meta = price.reset_index().groupby("ticker")
min_date, max_date = meta.min()["date"], meta.max()["date"]

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
    strategy=Trend_SMARatio,
    pipeline=VA_Ranking,
    calendar=calendar,
    **config,
)

backtest.add_base(
    ticker="carry",
    data=carry[ohlc],
)

for ticker in price.ticker.unique():
    data = price[price.ticker == ticker]
    data["liquidity"] = data.liquidity.ewm(alpha=0.05).mean()

    kwargs = {
        "inception": min_date[ticker],
        "maturity": max_date[ticker],
    }

    backtest.add_asset(
        ticker=ticker,
        data=adjust_stocks(
            data[ohlcrv],
        )[ohlcv],
        **kwargs,
    )


res = backtest.run()

##########################################################
################### RESULTS MANAGEMENT ###################

strat_meta = res["meta"].iloc[0, :]
df, rec = res["quotas"], res["records"]

print("\n" + str(strat_meta))
print("\n" + str(df))
print("\n" + str(rec))
