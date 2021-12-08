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
    Buy_n_Hold,
    Sell_n_Hold,
    SMARatio,
)
from backtesthub.pipelines.pipeline import (
    VA_Ranking,
    Rolling,
)
from backtesthub.strategy import Strategy
from backtesthub.backtest import Backtest
from backtesthub.calendar import Calendar
from backtesthub.utils.bases import Line
from backtesthub.utils.math import adjust_stocks
from backtesthub.utils.config import (
    _DEFAULT_SDATE,
    _DEFAULT_EDATE,
    _DEFAULT_URL,
)

pd.options.mode.chained_assignment = None

######################### CONFIG #########################

factor = "ALTBETA"
market = "STKBR@MOM"
hbase = "IBOV"
asset, hedge = "MULTI", "IND"
volume = "VOBZBRC"
ohlc = ["open", "high", "low", "close"]
ohlcr = ["open", "high", "low", "close", "returns"]
ohlcv = ["open", "high", "low", "close", "liquidity"]
ohlcrv = ["open", "high", "low", "close", "returns", "liquidity"]

config = {
    "factor": factor,
    "market": market,
    "asset": asset,
    "hedge": hedge,
    "hbase": hbase,
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
            ind = self.I(
                data=self.assets[asset],
                func=SMARatio,
                **self.params,
            )

            signal = self.I(
                data=self.assets[asset],
                func=Buy_n_Hold,
                **self.params,
            )

            volatility = self.V(
                data=self.assets[asset],
            )

            self.assets[asset].add_line(
                name="indicator",
                line=ind,
            )

            self.assets[asset].add_line(
                name="signal",
                line=Line(array=signal),
            )
            
            self.assets[asset].add_line(
                name="volatility",
                line=Line(array=volatility),
            )

    def next(self):
        univ = self.get_universe()

        for asset in univ:
            self.order_target(
                data=asset,
                target=self.sizing(
                    data=asset,
                )/len(univ),
            )


class Hedge_Beta(Strategy):
    params = {}

    def init(self):

        signal = self.I(
            data=self.hbase,
            func=Sell_n_Hold,
            **self.params,
        )

        volatility = self.V(
            data=self.hbase,
        )

        self.hbase.add_line(
            name="signal",
            line=Line(array=signal),
        )
        
        self.hbase.add_line(
            name="volatility",
            line=Line(array=volatility),
        )

        self.broadcast(
            base=self.hbase,
            assets=self.assets,
            lines=["signal", "volatility"],
        )

    def next(self):
        univ = self.get_universe()
        expo = self.get_tbeta()
        texpo = expo / len(univ)

        for hedge in univ:
            self.order(
                data=hedge,
                size=self.sizing(
                    data=hedge,
                    texpo=texpo,
                    method="EXPO",
                ),
            )


##########################################################
##################  DATABASE OPERATIONS ##################

engine = create_engine(
    URL.create(**_DEFAULT_URL),
    pool_pre_ping=True,
    echo=False,
)

base_meta = os.getenv("BASE_META")
base_hist = os.getenv("BASE_HIST")
comm_meta = os.getenv("COMM_META")
fut_meta = os.getenv("FUT_META")
fut_hist = os.getenv("FUT_HIST")
stocks_hist = os.getenv("STK_HIST")

hbase_sql = (
    "SELECT date, open, high, low, close "
    f"FROM {base_hist} "
    f"WHERE ticker = '{hbase}' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)


carry_sql = (
    "SELECT date, open, high, low, close "
    f"FROM {base_hist} "
    f"WHERE ticker = 'CARRY' AND date between "
    f"'{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

price_sql = (
    "SELECT date, ticker, open, high, low, close, "
    "returns, liquidity "
    f"FROM {stocks_hist} "
    f"WHERE date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

hmeta_sql = (
    "SELECT f.ticker as ticker, c.currency as curr, c.multiplier as mult, "
    f"f.endDate as mat FROM {comm_meta} c "
    f"INNER JOIN {fut_meta} f ON c.ticker = f.commodity "
    f"WHERE c.ticker IN ('{hedge}') AND f.endDate > '{_DEFAULT_SDATE}' "
    "ORDER BY mat"
)

hprice_sql = (
    "SELECT ticker, date, open, high, low, close "
    f"FROM {fut_hist} f "
    f"WHERE f.commodity = '{hedge}' AND "
    f"date between '{_DEFAULT_SDATE}' AND '{_DEFAULT_EDATE}'"
)

hbprice = pd.read_sql(hbase_sql, engine)
hprice = pd.read_sql(hprice_sql, engine)
hmeta = pd.read_sql(hmeta_sql, engine)
price = pd.read_sql(price_sql, engine)
carry = pd.read_sql(carry_sql, engine)

hbprice.set_index("date", inplace=True)
hprice.set_index("date", inplace=True)
hmeta.set_index("ticker", inplace=True)
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
        max(max_date),
    ),
    country="BR",
)

backtest = Backtest(
    strategy=Trend_SMARatio,
    pipeline=VA_Ranking,
    calendar=calendar,
    **config,
)

backtest.config_hedge(
    pipeline=Rolling,
    strategy=Hedge_Beta,
)

backtest.add_base(
    ticker="carry",
    data=carry[ohlc],
)

backtest.add_base(
    ticker=hbase,
    data=hbprice[ohlc],
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

for hticker, hprop in hmeta.iterrows():
    mask = hprice.ticker == hticker
    data = hprice[mask]

    commkwargs = dict(
        multiplier=hprop.mult,
        currency=hprop.curr,
        maturity=hprop.mat,
    )

    backtest.add_hedge(
        ticker=hticker,
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
