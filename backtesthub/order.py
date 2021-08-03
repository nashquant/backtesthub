#! /usr/bin/env python3

import numpy as np
import pandas as pd
from .utils.static import *
from dataclasses import dataclass
from typing import List, Optional
from warnings import filterwarnings
from .broker import Broker

filterwarnings('ignore')


class Order:
    
    __broker: Broker
    __size: float
    __price: float

    @property
    def size(self) -> float:
        """
        Order size (negative for short orders).
        If size is a value between 0 and 1, it is interpreted as a fraction of current
        available liquidity (cash plus `Position.pl` minus used margin).
        A value greater than or equal to 1 indicates an absolute number of units.
        """
        return self.__size

    @property
    def price(self) -> Optional[float]:
        """
        
        """
        return self.__price
