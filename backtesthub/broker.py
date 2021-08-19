#! /usr/bin/env python3

from numbers import Number
from datetime import date
from warnings import warn
from typing import Dict, Callable, Optional, Sequence, Union

from .order import Order
from .position import Position

from .utils.bases import Line, Asset, Hedge
from .utils.config import (
    _DEFAULT_BUFFER,
    _DEFAULT_CASH,
    _DEFAULT_STEP,
    _COMMTYPE,
    _STATUS,
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

        self.__lines["__index"] = Line(array=index)
        self.__lines["cash"] = Line(
            array=[_DEFAULT_CASH] * len(index),
        )
        self.__lines["equity"] = Line(
            array=[_DEFAULT_CASH] * len(index),
        )

        self.__orders: Dict[str, Order] = {}
        self.__cancels: Sequence[Order] = []
        self.__executed: Sequence[Order] = []
        self.__positions: Dict[str, Position] = {}
        self.__buffer: int = _DEFAULT_BUFFER

    def next(self):
        for order in self.__orders.values():
            status = order.status
            if status == _STATUS["W"]:
                self.execute_order(order)


    def new_order(
        self,
        data: Union[Asset, Hedge],
        size: Optional[float] = 0,
        limit: Optional[float] = None,
    ):
        pending = self.__orders.get(data.ticker)

        if pending is not None:
            self.cancel_order(ticker=data.ticker)

        order = Order(data.ticker, size, limit)
        self.__orders.update({data.ticker: order})

    def execute_order(
        self,
        order: Order
    ):
        cash = self.cash
        data = order.data
        margin = data.margin
        comm = data.commission
        commtype = data.commtype

        if commtype == _COMMTYPE["STOCKS"]:
            pass
        



    def cancel_order(
        self,
        ticker: str,
    ):
        cancel = self.__orders.pop(ticker, None)
        if cancel is None: 
            return
        
        if cancel.status == _STATUS["W"]:
            warn(f"Order cancelled: {ticker}")

        cancel.status = _STATUS["C"]
        self.__cancels.append(cancel)

    def __repr__(self):
        kls = self.__class__.__name__
        pos = {k: _pos.size for k, _pos in self.__positions.items()}
        ord = {k: _ord.size for k, _ord in self.__orders.items()}

        csh = self.cash
        eqt = self.equity

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
    def cash(self) -> Number:
        return self.__lines['cash'][0]

    @property
    def equity(self) -> Number:
        return self.__lines['equity'][0]

    @property
    def positions(self) -> Dict[str, Position]:
        return self.__positions

    @property
    def orders(self) -> Dict[str, Order]:
        return self.__orders

    @property
    def index(self) -> Line:
        return self.__lines.get("__index")
