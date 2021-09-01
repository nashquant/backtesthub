#! /usr/bin/env python3

from datetime import date
from numbers import Number

from typing import Optional, Union
from .utils.bases import Asset, Hedge
from .utils.config import _STATUS, _COMMTYPE


class Order:
    def __init__(
        self,
        data: Union[Asset, Hedge],
        size: Number,
        limit: Optional[Number] = None,
    ):
        self.__data = data
        self.__size = size
        self.__limit = limit
        self.__status: str = _STATUS["WAIT"]

        self.__isbuy: bool = self.__size > 0
        self.__issell: bool = self.__size < 0

        if not isinstance(data, (Asset, Hedge)):
            msg = "Order `data` must be either an Asset or a Hedge instance!"
            raise TypeError(msg)

        if not self.__isbuy and not self.__issell:
            msg = "Order `size` must be an non-zero number"
            raise ValueError(msg)

        self.__ticker: str = data.ticker
        self.__issue_date: date = data.date
        self.__exec_date: Optional[date] = None
        self.__side: int = 1 if self.__isbuy else -1

    def __repr__(self):
        kls = self.__class__.__name__
        tck = self.__ticker
        sze = self.__size
        sts = self.__status
        idt = self.__issue_date.isoformat()

        log = f"{kls}(Ticker: {tck}, Size: {sze}, Status: {sts}, Issued: {idt})"

        return log

    def cancel(self):
        self.__status = _STATUS["CANC"]

    @property
    def issue_date(self) -> date:
        return self.__issue_date
    
    @property
    def exec_date(self) -> Optional[date]:
        return self.__exec_date

    @exec_date.setter
    def exec_date(self, date: date):
        self.__exec_date = date

    @property
    def status(self) -> str:
        return self.__status

    @status.setter
    def status(self, status: str) -> str:
        self.__status = status

    @property
    def size(self) -> Number:
        return self.__size

    @property
    def ticker(self) -> str:
        return self.__ticker
    
    @property
    def data(self) -> Union[Asset, Hedge]:
        return self.__data

    @property
    def dt(self) -> date:
        return self.__data.index[0]

    @property
    def side(self) -> int:
        return self.__side

    @property
    def total_comm(self) -> Number:
        """
        Total Commission (Absolute $) 
        """
        comm = self.__data.commission
        commtype = self.__data.commtype

        if commtype == _COMMTYPE["PERC"]:
            exec_price = self.exec_price 
            if exec_price is None: 
                return 0
            comm = exec_price * comm

        return - comm * abs(self.__size)

    @property
    def exec_price(self) -> Number:
        """
        OBS: Some authors bound the
        value of the executed prices
        to be between the high/low
        prices.

        I choose not to do so, because
        it is more conservative to allow
        for big slippages, and because
        some data lack high and low,
        therefore it would underestimate
        the potential slippage.
        """

        if not self.__limit:
            slip = self.__data.slippage
            price = self.__data.open[0]

        else:
            open = self.__data.open[0]
            high = self.__data.high[0]
            low = self.__data.low[0]
            limit = self.__limit

            if self.__side == 1:
                if limit >= low:
                    price = min(limit, high, open)
                else:
                    return
            elif self.__side == -1:
                if limit <= high:
                    price = max(limit, high, open)
                else:
                    return

        return price * (1 + self.__side * slip)
