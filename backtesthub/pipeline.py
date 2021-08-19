#! /usr/bin/env python3

from datetime import date
from operator import itemgetter
from typing import Dict, Sequence
from collections import OrderedDict
from abc import ABCMeta, abstractmethod

from .utils.bases import Asset, Hedge


class Pipeline(metaclass=ABCMeta):

    """
    Pipeline is responsible for setting up
    the `simulation case` and recursively
    build a sequence called `universe`, which
    provides, for a given date, the list of
    all tradeable assets.

    The `self.universe` attribute may be either
    assigned at `init` if it is supposed to be
    static and defined before the simulation
    begins. Or it can be dynamically defined
    as next progresses.

    """

    def __init__(
        self,
        assets: Dict[str, Asset] = OrderedDict(),
        hedges: Dict[str, Hedge] = OrderedDict(),
    ):
        self.__assets = assets
        self.__hedges = hedges

    @abstractmethod
    def init(self):
        """ """

    @abstractmethod
    def next(self) -> Sequence[Asset]:
        """ """

    @property
    def asset(self) -> Asset:
        return self.__assets.values()[0]

    @property
    def assets(self) -> Dict[str, Asset]:
        return self.__assets

    @property
    def hedges(self) -> Dict[str, Hedge]:
        return self.__hedges

    @staticmethod
    def build_chain(self) -> Dict[str, date]:

        maturities: Dict[str, date] = {
            asset.ticker: asset.maturity for asset in self.__assets
        }

        chain: Dict[str, date] = dict(
            sorted(
                maturities.items(),
                key=itemgetter(1),
            )
        )

        return chain
