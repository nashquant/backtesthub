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

        if self.date > self.get_date(lag=1).weekday():

            self.actives = {
                asset.ticker: asset
                for asset in self.assets.values()
                if asset.inception <= date
                and asset.maturity >= date
                and asset.liquidity[0] > self._DEFAULT_LIQTHRESH
                and asset.volatility[0] < self._DEFAULT_STKMAXVOL
                and asset.volatility[0] > self._DEFAULT_STKMINVOL
            }

            self.liquid = dict(
                sorted(
                    [
                        (asset.ticker, asset.liquidity[0])
                        for asset in self.actives.values()
                        if asset.liquidity[0] and not np.isnan(asset.liquidity[0])
                    ],
                    key=lambda x: x[1],
                    reverse=True,
                )
            )

            self.rank = dict(
                sorted(
                    [
                        (asset.ticker, asset.indicator[0])
                        for asset in self.liquid.values()
                        if asset.indicator[0] and not np.isnan(asset.indicator[0])
                    ],
                    key=lambda x: x[1],
                    reverse=True,
                )
            )

            univ, names = list(), list()

            if not self.rank:
                return univ
            if not self.params.n:
                return univ

            while len(univ) < self.n and self.rank:

                ticker = self.rank.pop()
                name = ticker[:4]
                self.tk = ticker

                if name not in names:
                    univ.append(ticker)
                    names.append(name)

        return self.universe
