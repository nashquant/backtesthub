#! /usr/bin/env python3

from numbers import Number
from typing import Any, Dict, List
from warnings import filterwarnings
from dataclasses import dataclass, field

from .broker import Broker
from utils.config import _METHOD
from .utils.types import Asset, Line

filterwarnings('ignore')

@dataclass
class Position:

    """
    
    * `Position` data structure provides position 
      management and sizing methods/attributes.

    * `__ticker` differs from `asset` for futures-like
      
       
    
    """

    m = _METHOD["FIXED"]
    
    __ticker: field(default = "")
    __asset: field(default = "")
    __size: Number = field(default = 0)
    __target: Number = field(default = 0)
    __method: str = field(default = m)
    __signal: Number = field(default = 0)
    __stocklike: bool = field(default = True)
    __hedgelike: bool = field(default = True)

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
    def get_signal(self) -> float:
        
        """

        Expose `self.__signal` to client
        
        """
        
        return self.__signal

    def update(self, ticker):

        """

        Update Target Sizing 
        
        """

        pass

