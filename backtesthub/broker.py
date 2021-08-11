#! /usr/bin/env python3
from typing import Dict
from numbers import Number

from order import Order
from position import Position


class Broker:
    def __init__(
        self,
        buffer: int = 0,
        cash: Number = float("10e6"),
    ):

        self.__cash = [
            cash,
        ] * (buffer + 1)
        self.__equity = [
            cash,
        ] * (buffer + 1)
        self.__quotas = [
            1000,
        ] * (buffer + 1)

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
