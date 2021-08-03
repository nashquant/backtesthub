#! /usr/bin/env python3

from .utils.static import *
from dataclasses import dataclass
from typing import Dict, Optional
from warnings import filterwarnings
from .broker import Broker

filterwarnings('ignore')

@dataclass
class Position:
    
    __broker: Broker
    __size: Dict[str, int]

    def get_size(self, ticker: str) -> float:
        
        """
        
        """

        if ticker not in set(self.__size.keys()):
            self.__size.update({ticker: 0})

        
        return self.__size[ticker]
