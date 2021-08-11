#! /usr/bin/env python3

from datetime import date
from numbers import Number

from typing import Optional, Union
from .utils.types import Asset, Hedge
from .utils.config import _OTYPE, _STATUS

class Order:

    def __init__(
        self,
        data: Union[Asset, Hedge],
        size: Number = 0, 
        limit: Optional[Number] = None, 
        status: Optional[str] = _STATUS["W"],
    ):
        self.__data = data
        self.__size = size
        self.__limit = limit
        self.__status = status

        self.__isbuy = self.__size > 0
        self.__issell = self.__size < 0

        if not self.__isbuy and not self.__issell:
            msg = "Order `size` must be an non-zero number"
            raise ValueError(msg)

        if self.__limit is None:
            self.__otype = _OTYPE["M"]
        elif isinstance(self.__limit, Number):
            self.__otype = _OTYPE["L"]
        else:
            msg = "Couldn't identify order type"
            raise NotImplementedError(msg)

        self.__ticker = data.ticker
        self.__issdt = self.dt

    def __repr__(self):
        kls = self.__class__.__name__
        tck = self.__ticker
        typ = self.__otype
        sze = self.__size
        sts = self.__status
        idt = self.__issdt

        log = f"{kls}(Ticker: {tck}, Size: {sze}, Status: {sts}, Issued: {idt}, Type: {typ})"

        return log

    def __log(self, txt: str):
        msg = f"({self.dt.isoformat()}), {txt}"
        print(msg)

    @property
    def issdt(self) -> date:
        return self.__issdt

    @property
    def size(self) -> Number:
        return self.__size

    @property
    def ticker(self) -> str:
        return self.__ticker

    @property
    def dt(self) -> date:
        return self.__data.index[0]

    
