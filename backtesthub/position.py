#! /usr/bin/env python3

from numbers import Number
from typing import Union
from warnings import filterwarnings
from dataclasses import dataclass, InitVar, field

from utils.config import _METHOD
from utils.types import Asset, Hedge

filterwarnings('ignore')

@dataclass
class Position:

    """
    
    * `Position` data structure provides position 
      management and sizing methods/attributes.

    * `__ticker` differs from `asset` for futures-like
      
       
    """

    data: Union[Asset, Hedge] = field(compare = False, repr = False)
    
    __size: Number = field(init = False, default = 0, compare = False, repr = True)
    __signal: Number = field(init = False, default = 0, compare = False, repr = True)
    __target: Number = field(init = False, default = 0, compare = False, repr = False)
    __method: str = field(init = False, default = _METHOD["V"], compare = False, repr = False)

    def config(
        self,
        method: str
    ):

        if method not in _METHOD:
            msg = "Method not recognized"
            raise NotImplementedError(msg)

        self.__method = method
    
    def update(self, ticker):

        """

        Update Target Sizing 
        
        """

        pass

    def data(self) -> Union[Asset, Hedge]:

        """

        Expose `self.__data` to client
        
        """

        return self.__data

    
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
