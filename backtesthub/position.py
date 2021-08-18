#! /usr/bin/env python3

from typing import Union, Optional
from .utils.bases import Asset, Hedge

class Position:

    """
       
    """

    def __init__(
        self,
        data: Union[Asset, Hedge],
    ):

        if not isinstance(data, (Asset, Hedge)):
            msg = "Invalid Data Type"
            raise TypeError(msg)
            
        self.__data = data
        self.__size = 0

    def __repr__(self):
        kls = self.__class__.__name__
        tck = self.__data.ticker
        sig = self.__data.signal[0]
        siz = self.__size

        log = f"{kls}(Ticker: {tck}, Size: {siz}, Signal: {sig})"

        return log

    def target(self):
        pass
    

    @property
    def size(self) -> float:
        
        """

        Expose `self.__size` to client
        
        """
        
        return self.__size

    @property
    def signal(self) -> float:
        
        """

        Expose `self.__signal` to client
        
        """
        
        return self.__data.signal[0]

    @property
    def ticker(self) -> float:
        
        """

        Expose `self.__ticker` to client
        
        """
        
        return self.__data.ticker
