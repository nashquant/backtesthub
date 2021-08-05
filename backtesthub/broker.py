#! /usr/bin/env python3

import numpy as np
import pandas as pd
from typing import List, Dict, Optional
from warnings import filterwarnings
from dataclasses import dataclass

from .utils.types import *
from .order import Order

filterwarnings("ignore")

@dataclass
class Broker:

    __datas: Dict[str, Asset]
    __cash: Optional[float]

    @property
    def comm(self):

        """
        Comission Values
        """

        return self.__comm

    @property
    def cash(self):

        """
        Total Cash Balance
        """

        return self.__equity
    
    @property
    def cash(self):

        """
        Total Cash Balance
        """

        return self.__cash
    
    @property
    def lev(self):

        """
        Total Leverage
        """

        if not self.__margin:
            return 1

        return 1/self.__margin

    @property
    def oprc(self) -> float:
        
        """
        Open close price
        """

        return self.__dnames[self.__tk].Open[0]
    
    @property
    def prc(self) -> float:
        
        """
        Current close price
        """

        return self.__dnames[self.__tk].Close[0]

    @property
    def lprc(self) -> float:
        
        """
        Last close price
        """

        return self.__dnames[self.__tk].Close[-1]


    def _issue_order(
        self,
        ticker: str,
        size: float,
        limit: float = None,
    ):

        order = Order(
            self, 
            ticker, 
            size, 
            limit
        )
        
        self.__orders.insert(0, order)

    def _process_orders(self):
        pass

    def next(self):
        pass
    
    

