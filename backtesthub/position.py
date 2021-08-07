#! /usr/bin/env python3

from numbers import Number
from warnings import filterwarnings
from typing import Union, Dict, List
from dataclasses import dataclass, field

from .broker import Broker
from utils.config import _METHOD
from .utils.types import Asset, Hedge,Line

filterwarnings('ignore')

@dataclass
class Position:

    """
    
    * `Position` data structure provides position 
      management and sizing methods/attributes.

    * `__ticker` differs from `asset` for futures-like
      
       
    
    """
    
    __ticker: str = field(default = "", compare = False, repr = True)
    __asset: Union[Asset, Hedge] =  field(default = "", compare = True, repr = True)
    __size: Number = field(default = 0, compare = False, repr = True)
    __target: Number = field(default = 0, compare = False, repr = False)
    __method: str = field(default = _METHOD["V"], compare = False, repr = False)
    __signal: Number = field(default = 0, compare = False, repr = True)
    __stocklike: bool = field(default = True, compare = False, repr = True)
    __hedgelike: bool = field(default = True, compare = True, repr = True)

    
    def set_position(
        self,
        ticker: str, 
        asset: str = None,
        size: Number = 1  
    ):

        self.__ticker = ticker
        self.__size = 
        self.__asset = asset if asset else ticker

    def set_method(
        self,
        method: str
    ):

        if method not in _METHOD:
            msg = "Method not recognized"
            raise NotImplementedError(msg)
    
    def update(self, ticker):

        """

        Update Target Sizing 
        
        """

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
