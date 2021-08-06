#! /usr/bin/env python3

from collections import deque

from numbers import Number
from warnings import filterwarnings
from typing import List, Dict, Optional
from dataclasses import dataclass, field

from .utils.types import *
from .order import Order
from .position import Position

filterwarnings("ignore")

@dataclass
class Broker:

    __cash: Number = field(default = {}, compare = True, repr = True)
    __equity: Number = field(default = {}, compare = True, repr = True)
    __orders: List = field(default = [], compare = True, repr = True)
    __datas: Dict[str, Asset] = field(default = {}, compare = False, repr = False)
    __positions: Dict[str, Position] = field(default = {}, compare = True, repr = True)

    def __init__(self, **kwargs):

        self.__cash = kwargs.get('cash')
        self.__equity = kwargs.get('cash')
        self.__datas = kwargs.get('datas')


    def order(
        self,
        ticker: str,
        size: float,
        limit: float,
    ):

        order = Order( 
            ticker, 
            size, 
            limit
        )
        
        self.__orders.append(order)

    def __process_orders(self):
        pass

    def next(self):
            
        self.__process_orders()


    @property
    def equity(self):

        """
        Total Equity Balance
        """

        return self.__equity
    
    @property
    def cash(self):

        """
        Total Cash Balance
        """

        return self.__cash

    @property
    def position(self):

        """
        Current Positions
        """

        return self.__positions
    
    @property
    def orders(self):

        """
        Current Orders
        """

        return self.__orders
    
    

