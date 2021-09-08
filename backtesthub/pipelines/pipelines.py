#! /usr/bin/env python3

import numpy as np
from typing import Sequence
from datetime import date

from ..pipeline import Pipeline
from ..utils.bases import Asset


class Single(Pipeline):
    def init(self):
        self.universe = tuple(self.assets.values())

    def next(self) -> Sequence[Asset]:
        return self.universe


class Rolling(Pipeline):
    def init(self):
        self.build_chain(), self.apply_roll()
        self.universe = []

    def next(self) -> Sequence[Asset]:

        while self.get_date() > self.maturity:
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

    def init(self):
        self.build_chain()
        self.curr = self.chain[-1]
        self.ref_year = self.curr.maturity.year

    def next(self) -> Sequence[Asset]:

        while self.get_date() > self.roll_date:
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
    )

    params = {"n": 30}

    def init(self):
        self.universe = []

    def next(self) -> Sequence[Asset]:

        if self.date.weekday() > self.get_date(lag=1).weekday():

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

            while len(unv) < self.params["n"] and tmp:
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
    )

    params = {"n": 30}

    def init(self):
        self.universe = []

    def next(self) -> Sequence[Asset]:

        if self.date.weekday() > self.get_date(lag=1).weekday():

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

            while len(unv) < self.params["n"] and tmp:
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
