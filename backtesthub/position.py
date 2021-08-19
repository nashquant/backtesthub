#! /usr/bin/env python3

from typing import Union, Optional
from numbers import Number

from .utils.bases import Asset, Hedge

class Position:

    """
       
    """

    def __init__(
        self,
        size: Number,
        data: Union[Asset, Hedge],
        stop: Optional[Number] = None,
    ):

        if not isinstance(data, (Asset, Hedge)):
            msg = "Invalid Data Type"
            raise TypeError(msg)
            
        self.__data = data
        self.__stop = stop
        self.__size = size

    def __repr__(self):
        kls = self.__class__.__name__
        tck = self.__data.ticker
        sig = self.__data.signal[0]
        siz = self.__size

        log = f"{kls}(Ticker: {tck}, Size: {siz}, Signal: {sig})"

        return log

    def check_stop(self):
        pass
    

    @property
    def size(self) -> float:
        return self.__size

    @property
    def expo(self) -> float:
        mult = self.data.multiplier
        price = self.data.close[0]

        return self.__size * mult * price 

    @property
    def perc_margin(self) -> float:
        margin = self.data.margin
        if not margin: return 0

        return margin * self.expo 

    @property
    def stop(self) -> Optional[float]:
        return self.__stop

    @property
    def signal(self) -> float:
        return self.__data.signal[0]

    @property
    def ticker(self) -> float:
        return self.__data.ticker
