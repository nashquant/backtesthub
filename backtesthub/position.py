#! /usr/bin/env python3

from typing import Union, Optional
from numbers import Number

from .utils.bases import Asset, Hedge

class Position:

    """
    `Position Class`
       
    """

    def __init__(
        self,
        data: Union[Asset, Hedge],
        size: Number,
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
        raise NotImplementedError()

    def add(self, delta: int):
        if not type(delta) == int:
            msg="Wrong input for position delta"
            raise TypeError(msg)

        self.__size+=delta     

    @property
    def expo(self) -> float:
        """
        Exposition
        """
        mult = self.data.multiplier
        price = self.data.close[0]

        return self.__size * mult * price

    @property
    def stop(self) -> Optional[float]:
        return self.__stop

    @property
    def ticker(self) -> float:
        return self.__data.ticker

    @property
    def signal(self) -> float:
        return self.__data.signal[0]

    @property
    def size(self) -> float:
        return self.__size
    
    @property
    def data(self) -> Union[Asset, Hedge]:
        return self.__data
