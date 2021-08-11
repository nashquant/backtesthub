#! /usr/bin/env python3

from typing import Union, Optional

from .utils.config import _METHOD
from .utils.types import Asset, Hedge

class Position:

    """
       
    """

    def __init__(
        self,
        data: Union[Asset, Hedge],
        method: Optional[str] = "V"
    ):
        if method not in _METHOD:
            msg = "Method not recognized"
            raise NotImplementedError(msg)

        self.__method = _METHOD[method]

        if not isinstance(data, (Asset, Hedge)):
            msg = "Invalid Data Type"
            raise TypeError(msg)
            
        self.__data = data
        self.__ticker = data.ticker
        self.__stocklike = data.stocklike
        
        self.__size = None
        self.__signal = None
        self.__target = None
        self.__avgprc = None

    def __repr__(self):
        kls = self.__class__.__name__
        tck = self.__data.ticker
        sze = self.__size or ""
        sig = self.__signal or ""
        avg = self.__avgprc or ""
        tgt = self.__target or ""

        log = f"{kls}(Ticker: {tck}, Size: {sze}, Signal: {sig}, Avg Prc: {avg}, Target: {tgt})"

        return log

    def update(self):

        """

        Update Target Sizing 
        
        """

        self.__get_signal()
        self.__get_target()
        
        if not self.__stocklike:
            self.__roll()


    def __get_signal(self):
        pass

    def __get_target(self):
        pass
    
    @property
    def target(self) -> float:
        
        """

        Expose `self.__target` to client
        
        """
        
        return self.__target

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
        
        return self.__signal

    @property
    def ticker(self) -> float:
        
        """

        Expose `self.__ticker` to client
        
        """
        
        return self.__ticker
