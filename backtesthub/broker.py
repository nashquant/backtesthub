#! /usr/bin/env python3
from typing import Dict
from numbers import Number

from .order import Order
from .position import Position


class Broker:
    def __init__(
        self, 
        cash: Number = float("10e6"), 
        curr: str = "BRL"
    ):
        self.__cash = [cash]
        self.__equity = [cash]
        self.__quotas = [1000]

        self.__curr = curr

        self.__orders: Dict[str, Order] = {}
        self.__positions: Dict[str, Position] = {}

    def __repr__(self):
        kls = self.__class__.__name__
        pos = {k: v.size for k, v in self.__positions.items()}
        ord = {k: v.size for k, v in self.__orders.items()}

        csh = self.__cash[-1]
        eqt = self.__equity[-1]

        log = f"{kls}(Cash: {csh}, Equity: {eqt}, Positions: {pos}, Orders: {ord})"

        return log

    def order(
        self,
        ticker: str,
        size: float,
        limit: float,
    ):
        order = Order(ticker, size, limit)
        self.__orders.update({ticker: order})

    def __process_orders(self):
        pass

    def next(self):
        self.__process_orders()

    @property
    def equity(self):
        return self.__equity

    @property
    def cash(self):
        return self.__cash

    @property
    def position(self):
        return self.__positions

    @property
    def orders(self):
        return self.__orders
