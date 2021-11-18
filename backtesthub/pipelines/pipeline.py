#! /usr/bin/env python3

import numpy as np
from typing import Optional, Sequence
from datetime import date

from ..pipeline import Pipeline
from ..utils.bases import Asset


class Single(Pipeline):
    """
    `Single Pipeline`

    Extends from the Base Pipeline Class.

    This single pipeline is a "degenerate case", i.e., it basically 
    just returns the universe as equals to the  full assets set . It's 
    named "single" because it is most useful when backtesting a single 
    asset strategy.
    
    """

    def init(self):
        self.universe = tuple(self.assets.values())

    def next(self) -> Sequence[Asset]:
        return self.universe


class Rolling(Pipeline):
    """
    `Rolling Pipeline`

    Extends from the Base Pipeline Class.

    Rolling pipeline is designed to make front month futures rolling, 
    i.e., it assumes the closest to maturity contract will always be
    the active - and the one we're interested in trading. 
    
    It returns as the universe a [list of] single asset, and does the 
    job of monitoring the rolling date, in order to update the universe
    and close positions in older contracts. 
    
    It is expected to be used only for futures derivatives whose
    liquidity is concentrated in the front contract.

    NOTE: Rolling is triggered a number "LAG" of days prior maturity.
    Users can configure the value of "LAG" by changing the value of
    "DEF_LAG" environment variable.

    
    """
    def init(self):
        self.build_chain(), self.apply_roll()
        self.universe = []

    def next(self) -> Sequence[Asset]:

        while self.get_lagged_date() > self.maturity:
            self.broker.close(self.curr)
            self.apply_roll()

        return self.universe

    def apply_roll(self):
        if not self.chain:
            msg = "Empty chain"
            raise ValueError(msg)

        self.curr = self.chain.pop()
        self.universe = [self.curr]
        self.maturity = self.curr.maturity


class Vertice(Pipeline):

    from ..utils.config import _DEFAULT_RATESDAY, _DEFAULT_RATESMONTH

    """
    `Vertice Pipeline`

    Extends from the Base Pipeline Class.

    Vertice pipeline is designed to make vertice-style futures trading, 
    i.e., it assumes that the user is interested in trade specific 
    tenors of the future curve, specially useful for rates trading. 
    
    It doesn't formally returns the tradeable universe, but instead
    returns the chain of active assets (ones that have not expired), 
    and let the "strategy" perform the "vertice/tenor" picking rule. 
    
    It is useful to feed the backtest engine only with tradeable
    tenors. For example, for the Brazilian DI futures, we're mostly
    interested in trading january tenors when it comes to the long
    end of the curve. Thus, we should be only feeding the "F" [jan]
    contracts to the engine - See ~/examples/rates.py.

    NOTE: Rolling is triggered a number "LAG" of days prior maturity.
    Users can configure the value of "LAG" by changing the value of
    "DEF_LAG" environment variable.
    
    """

    def init(self):
        self.build_chain()
        self.curr = self.chain[-1]
        self.ref_year = self.curr.maturity.year

    def next(self) -> Sequence[Asset]:

        while self.get_lagged_date() > self.roll_date:
            self.broker.close(self.curr)
            self.apply_roll()

        return self.chain

    def apply_roll(self):
        if not self.chain:
            msg = "Empty chain"
            raise ValueError(msg)

        self.chain.pop()
        self.curr = self.chain[-1]
        self.ref_year = self.curr.maturity.year

    @property
    def roll_date(self) -> date:
        return date(
            self.ref_year,
            self._DEFAULT_RATESMONTH,
            self._DEFAULT_RATESDAY,
        )


class Ranking(Pipeline):

    from ..utils.config import (
        _DEFAULT_STKMINVOL,
        _DEFAULT_STKMAXVOL,
        _DEFAULT_LIQTHRESH,
        _DEFAULT_N
    )

    """
    `Ranking Pipeline`

    Extends from the Base Pipeline Class.

    Ranking pipeline is designed to make factor-style stock picking, 
    i.e., it assumes that the user is interested in rank stocks by 
    some common factor, and select a subset of the best ranked ones
    to buy [and eventually a subset of the worst ranked to short].

    This pipeline has two default screening stages:

    1) Eliminate stocks that are not tradable, either due to practical
    impediments, such as de-listing / target M&A (maturity date needs to 
    be assigned) or the case where the stock was not listed yet (but will 
    be available later - inception date needs to be assigned), or because 
    their volatility at the time is too low (lower than "_DEF_STKMINVOL") 
    or too high (higher than "_DEF_STKMAXVOL")

    2) After ranking stocks by their data.indicator[0] values, the algo
    make sure it is not making duplicate positions, for example, trading 
    same company ordinary/preferential stocks - for that we assume the
    ticker to be well behaved enough to represent two stocks of the same
    company as having the same initial four letters, as it is common in
    the brazilian market (if this hypothesis does not hold, changes need
    to be implemented in this function!) - e.g. PETR3 and PETR4 will be
    considered stocks belonging to the same company, thus just the best 
    ranked one will be select.  
    
    After this screening stage, this pipeline is designed to select a
    number ("_DEF_N") of the best ranked stocks, which by default is
    set to 30. The selection of a fixed subset is consistent with the
    idea of reduced trading activity (compared to quantile selection
    which leads to frequent rebalances to maintain risk parity), while 
    maintaining a good level of diversification, but not too much 
    (30 is a reasonable level).
    
    We encourage users to feed the most complete set of stocks possible 
    (considering survivorship bias), as well as configure each stock
    inception and maturity dates. See ~/examples/multi_stocks.py.

    NOTE: We assume that this pipeline results' are sufficient for
    the "long" part of the portfolio, and we assume that stocks fed 
    belong to the same timezone/exchange, in order to avoid complications 
    regarding holidays and alike. For the "short" part, we encourage users
    to make a separate hedge-pipeline, as it was performed in the example.

    
    """

    def init(self, n: Optional[int] = None):
        self.universe = []
        self.n = n
        if self.n is None:
            self.n = self._DEFAULT_N

    def next(self) -> Sequence[Asset]:

        if self.date.weekday() > self.get_lagged_date(lag=1).weekday():

            self.actives = {
                asset.ticker: asset
                for asset in self.assets.values()
                if asset.inception <= self.date
                and asset.maturity >= self.date
                and asset.liquidity[0] > self._DEFAULT_LIQTHRESH
                and asset.volatility[0] < self._DEFAULT_STKMAXVOL
                and asset.volatility[0] > self._DEFAULT_STKMINVOL
            }

            self.rank = sorted(
                [
                    (asset.ticker, asset.indicator[0])
                    for asset in self.actives.values()
                    if not np.isnan(asset.indicator[0])
                ],
                key=lambda x: x[1],
            )

            unv, names = [], []
            tmp = [x[0] for x in self.rank]

            while len(unv) < self.n and tmp:
                ticker = tmp.pop()
                name = ticker[:4]
                self.tk = ticker

                if name not in names:
                    names.append(name)
                    unv.append(self.assets[ticker])

            for asset in self.universe:
                if asset not in unv:
                    self.broker.close(asset)

            self.universe = unv.copy()

        return self.universe


class VA_Ranking(Pipeline):

    from ..utils.config import (
        _DEFAULT_STKMINVOL,
        _DEFAULT_STKMAXVOL,
        _DEFAULT_LIQTHRESH,
        _DEFAULT_N,
    )

    """

    `Ranking Pipeline`

    Extends from the Base Pipeline Class.

    Very similar to Ranking Pipeline, with the slight modification
    to introduce the volatility adjusted indicator ranking.

    E.g. Instead of ranking by the ratio of ind = SMA(n1)/SMA(n2), 
    we'll perform a composite ranking, ind' = ind/volatility.

    This is useful to a plenty of cases, such as momentum ranking,
    where both past returns and past vol-adjusted returns (some 
    metric analogous to sharpe) could be employed as a signal.  

    """

    def init(self, n: Optional[int] = None):
        self.universe = []
        self.n = n
        if self.n is None:
            self.n = self._DEFAULT_N

    def next(self) -> Sequence[Asset]:

        if self.date.weekday() > self.get_lagged_date(lag=1).weekday():

            self.actives = {
                asset.ticker: asset
                for asset in self.assets.values()
                if asset.inception <= self.date
                and asset.maturity >= self.date
                and asset.liquidity[0] > self._DEFAULT_LIQTHRESH
                and asset.volatility[0] < self._DEFAULT_STKMAXVOL
                and asset.volatility[0] > self._DEFAULT_STKMINVOL
            }

            self.rank = sorted(
                [
                    (asset.ticker, asset.indicator[0], asset.volatility[0])
                    for asset in self.actives.values()
                    if not np.isnan(asset.indicator[0])
                ],
                key=lambda x: x[1] / x[2],
            )

            unv, names = [], []
            tmp = [x[0] for x in self.rank]

            while len(unv) < self.n and tmp:
                ticker = tmp.pop()
                name = ticker[:4]
                self.tk = ticker

                if name not in names:
                    names.append(name)
                    unv.append(self.assets[ticker])

            for asset in self.universe:
                if asset not in unv:
                    self.broker.close(asset)

            self.universe = unv.copy()

        return self.universe

class Portfolio(Pipeline):

    """
    `Portfolio Pipeline`

    Extends from the Base Pipeline Class. It works together 
    with Hierarchy Strategy defined at ~/examples/portfolio.py
    
    From the beggining of the fund we opted to have an Static 
    Hierarchy Risk Parity (HRP - there are lots of reference to 
    this topic), since it combined simplicity and flexibility to 
    our allocation process - we can control how much risk conctr 
    we give to each factor and market, without having to be very 
    specific in the allocation of the lower hierarchy levels (those 
    are pure risk parity based, i.e., allocation is the same for
    each "child" and defined by the upper level's risk budget). 
    
    This approach DOES NOT try to maximize the expected returns, 
    instead its based on the idea that, ex-ante, we cannot know 
    which models will outperform, but we acknowledge that models
    that belongs to same factor & market (in order, hierarchies 
    one and two) tend to share a lot of underlying risk factors,
    and some may be more interesting than others, especially if
    we account effects of liquidity, significantly higher and
    sustained realized sharpe ratio, etc. Thus, those models
    should be fighting among themselves to achieve a higher
    risk participation in the overall structure.

    Indeed, this model should be viewed as a similar approach
    of an inv. bank's prop desk , where traders (models) fight
    to get a higher allocation, while the organization defines
    the overall risk budget, and how it is divided in the upper
    levels so that it reflects best the desired risk profile 
    
    For instance, we like trend-following properties, so we want 
    our fund to have a higher share of it overall, that's why it
    receives >60% risk budget (highest level). However, for the 
    lower levels (i.e. market, geographies, etc.), it has a much 
    more balanced approach, where risk is divided almost equally 
    in the downstream branches.

    Here, we expect that all underlying strategies start and end
    at the same dates, but it may not occur, that's why we let
    the universe to rebuild at each `next` call.

    OBS: STILL PENDING DEVELOPMENT OF A RISK REDISTRIBUITION 
    SCHEME IN CASE STRATEGIES START/END IN DIFERENT PERIODS...   
    
    """

    def init(self):
        self.universe = []

    def next(self) -> Sequence[Asset]:
        self.universe = [
            asset for asset in self.assets.values()
            if asset.inception <= self.date
            and asset.maturity >= self.date
        ]
