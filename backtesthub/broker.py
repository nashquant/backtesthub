#! /usr/bin/env python3

from numbers import Number
from datetime import date
from typing import Dict, Callable, Optional, Sequence

from .order import Order
from .position import Position

from .utils.bases import Line
from .utils.config import (
    _DEFAULT_BUFFER,
    _DEFAULT_CASH,
    _DEFAULT_STEP,
)


class Broker:
    def __init__(
        self,
        index: Sequence[date],
        cash: Number = _DEFAULT_CASH,
    ):
        self.__index = index
        self.__buffer = _DEFAULT_BUFFER
        self.__lines: Dict[str, Line] = {}

        self.__lines["__index"] = Line(
            array=index
        )
        self.__lines["cash"] = Line(
            array=[_DEFAULT_CASH] * len(index),
        )
        self.__lines["equity"] = Line(
            array=[_DEFAULT_CASH] * len(index),
        )

        self.__orders: Dict[str, Order] = {}
        self.__positions: Dict[str, Position] = {}
        self.__buffer: int = _DEFAULT_BUFFER

    def next(self):
        self.__process_orders()

    def new_order(
        self,
        ticker: str,
        size: float,
        limit: float,
    ):
        order = Order(ticker, size, limit)
        self.__orders.update({ticker: order})

    def __process_orders(self):
        pass

    def __repr__(self):
        kls = self.__class__.__name__
        pos = {k: _pos.size for k, _pos in self.__positions.items()}
        ord = {k: _ord.size for k, _ord in self.__orders.items()}

        csh = self.cash[0]
        eqt = self.equity[0]

        log = f"{kls}(Cash: {csh}, Equity: {eqt}, Positions: {pos}, Orders: {ord})"

        return log

    def __getitem__(self, line: str):
        return self.__lines.get(line.lower())

    def __getattr__(self, line: str):
        return self.__lines.get(line.lower())

    def __len__(self):
        return len(self.__index)

    def __sync_buffer(func: Callable):
        def wrapper(self, *args, **kwargs):

            func(self, *args, **kwargs)

            for line in self.__lines.values():
                line._Line__next()

        return wrapper

    @__sync_buffer
    def next(self, step: int = _DEFAULT_STEP):
        self.__buffer = min(
            self.__buffer + step,
            len(self) - 1,
        )

    def get_position(self, ticker: str) -> Optional[Position]:
        return self.__positions.get(ticker)

    def get_orders(self, ticker: str) -> Optional[Position]:
        return self.__orders.get(ticker)

    @property
    def positions(self) -> Dict[str, Position]:
        return self.__positions

    @property
    def orders(self) -> Dict[str, Order]:
        return self.__orders

    @property
    def index(self) -> Line:
        return self.__lines.get("__index")