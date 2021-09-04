#! /usr/bin/env python3

from numbers import Number
from typing import Union, Optional
from .utils.bases import Asset

class Position:

    """
    `Position Class`
       
    """

    def __init__(
        self,
        data: Union[Asset],
        size: Number,
        stop: Optional[Number] = None,
    ):
        if not isinstance(data, Asset):
            msg = "Arg `Data` must be an Asset type"
            raise TypeError(msg)
            
        self.__data = data
        self.__stop = stop
        self.__size = size

    def __repr__(self):

        return (
            f"{self.__class__.__name__}(Ticker: {self.__data.ticker}, "
            f"Size: {self.__size}, Signal: {self.__data.signal[0]})"
        )

    def check_stop(self):
        raise NotImplementedError()

    def add(self, delta: Number):
        if not isinstance(delta, Number):
            msg="`Position Size must be a Number!"
            raise TypeError()
        
        self.__size+=delta     

    @property
    def data(self) -> Asset:
        return self.__data

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
