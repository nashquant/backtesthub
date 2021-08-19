#! /usr/bin/env python3

from datetime import date
from numbers import Number

from typing import Optional, Union
from .utils.bases import Asset, Hedge
from .utils.config import _STATUS, _COMMTYPE

class Order:

    def __init__(
        self,
        size: Number,
        data: Union[Asset, Hedge],
        limit: Optional[Number] = None, 
    ):
        self.__data = data
        self.__size = size
        self.__limit = limit
        self.__status = _STATUS["WAIT"]

        self.__isbuy = self.__size > 0
        self.__issell = self.__size < 0

        if type(data) not in (Asset, Hedge):
            msg = "Order `data` must be either an Asset or a Hedge"
            raise TypeError(msg)
            
        if not self.__isbuy and not self.__issell:
            msg = "Order `size` must be an non-zero number"
            raise ValueError(msg)

        self.__ticker = data.ticker
        self.__issue_date = data.date
        self.__side = 1 if self.__isbuy else -1

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
    def status(self) -> str:
        return self.__status

    @property
    def size(self) -> Number:
        return self.__size

    @property
    def ticker(self) -> str:
        return self.__ticker

    @property
    def dt(self) -> date:
        return self.__data.index[0]

    @property
    def side(self) -> int:
        return self.__side

    @property
    def commission(self) -> Number:
        comm = self.__data.commission
        commtype = self.__data.commtype

        if commtype == _COMMTYPE["ABS"]:
            return comm

        elif commtype == _COMMTYPE["PERC"]:
            return self.exec_price*comm

    @property
    def total_comm(self) -> Number:
        """
        Commission as absolute value
        """
        return self.commission * abs(self.__size)

    @property
    def total_margin(self) -> Number:
        """
        Margin as % of Expo value
        """
        return self.__data.margin * abs(self.__size)

    @property
    def exec_price(self) -> Number:

        """
        OBS: Some authors limit the
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
        
        slip = self.__data.slippage
        open = self.__data.open[0]

        return open * (1+self.side*slip) 


    
