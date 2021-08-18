#! /usr/bin/env python3

from datetime import date
from operator import itemgetter
from typing import Dict, Sequence, Union

from .utils.bases import Base, Asset, Hedge
from .utils.config import _PMETHOD


class Pipeline:

    """
    Pipeline is responsible for identifying
    the `simulation case` and recursively
    build a sequence called `universe`, which
    provides, for a given date, the list of
    all tradeable assets, for every run() call.

    This enables interesting features such as
    the broadcasting of a base signal to multiple
    assets, rolling operations for futures, etc.

    "Warning": Lots of cases aren't defined yet.
    We do have support for those:

    1) Base signal + Futures Rolling. - Must feed
    one base data, and a sequence of futures-like
    assets.

    2) Rates-like Vertice Trading - Must feed a
    sequence of rates-like(thus, futures-like too)
    and a

    3) Single Stocklike - ...

    4) Multi Stocklike - ...

    5) Portfolios - ...

    """

    def __init__(
        self,
        bases: Dict[str, Base] = {},
        assets: Dict[str, Asset] = {},
        hedges: Dict[str, Hedge] = {},
        case: Dict[str, bool] = {},
    ):
        self.__bases = bases
        self.__assets = assets
        self.__hedges = hedges
        self.__case = case

    def init(self):

        stocklike = self.__case.get("stocklike")
        rateslike = self.__case.get("rateslike")
        multiasset = self.__case.get("multiasset")

        h_stocklike = self.__case.get("h_stocklike")
        h_rateslike = self.__case.get("h_rateslike")
        h_multiasset = self.__case.get("h_multiasset")

        if rateslike:
            self.__method = _PMETHOD["VERT"]

        elif stocklike and multiasset:
            self.__method = _PMETHOD["RANK"]

        elif not stocklike and multiasset:
            self.__method = _PMETHOD["ROLL"]
            self.__chain = self.build_chain(
                assets = self.__assets,
            )

        elif stocklike and not multiasset:
            self.__method = _PMETHOD["DEFA"]

        else:
            msg = "Simulation Case couldn't be identified"
            raise NotImplementedError(msg)

        # if h_rateslike:
        #     self.__h_method = _PMETHOD["VERT"]

        # elif h_stocklike and h_multiasset:
        #     self.__h_method = _PMETHOD["RANK"]

        # elif not h_stocklike and h_multiasset:
        #     self.__h_method = _PMETHOD["ROLL"]
        #     self.__h_chain = self.build_chain(
        #         assets = self.__hedges,
        #     )

        # elif h_stocklike and not h_multiasset:
        #     self.__h_method = _PMETHOD["DEFA"]

        # else:
        #     msg = "Simulation Case couldn't be identified"
        #     raise NotImplementedError(msg)

    def run(
        self,
        date: date,
        old: Sequence[Asset],
    ) -> Sequence[Asset]:

        if self.__method == _PMETHOD["DEFA"]:
            return tuple(self.__assets.values())

        if self.__method == _PMETHOD["ROLL"]:
            pass

    @staticmethod
    def build_chain(
        assets: Sequence[Union[Asset,Hedge]],
    ) ->  Dict[str, date]:

        maturities: Dict[str, date] = {
            asset.ticker: asset.maturity for asset in assets
        }

        chain: Dict[str, date] = dict(
            sorted(
                maturities.items(),
                key=itemgetter(1),
            )
        )

        return chain
