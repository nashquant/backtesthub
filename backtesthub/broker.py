#! /usr/bin/env python3

from numbers import Number
from datetime import date
from warnings import warn
from typing import Dict, List, Optional, Sequence, Union

from .order import Order
from .position import Position

from .utils.bases import Line, Base, Asset, Hedge
from .utils.config import (
    _DEFAULT_BUFFER,
    _DEFAULT_CASH,
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

        self.__lines["__index"] = Line(
            array=index,
        )

        self.__lines["cash"] = Line(
            array=[cash] * len(index),
        )

        self.__lines["equity"] = Line(
            array=[cash] * len(index),
        )

        self.__lines["open"] = Line(
            array=[cash] * len(index),
        )

        self.__orders: Dict[str, Order] = {}
        self.__cancels: List[Order] = []
        self.__executed: List[Order] = []
        self.__currs: Dict[str, Base] = {}
        self.__positions: Dict[str, Position] = {}
        self.__buffer: int = _DEFAULT_BUFFER

    def add_carry(self, carry: Base):
        if isinstance(carry, Base):
            msg = "Wrong input type for carry"
            raise TypeError(msg)

        self.carry = carry

    def add_curr(self, curr: Base):
        if isinstance(curr, Base):
            msg = "Wrong input type for carry"
            raise TypeError(msg)

        self.__currs.update({curr.ticker: curr})

    def new_order(
        self,
        data: Union[Asset, Hedge],
        size: Optional[float] = 0,
        limit: Optional[float] = None,
    ):
        """
        `Order Creation`

        - Checks order for the same ticker.
        - If so, cancels that order.
        - Updates Open Orders Dictionary.

        """
        pending = self.__orders.get(data.ticker)

        if pending is not None:
            self.__cancel_order(pending)

        order = Order(data, size, limit)
        self.__orders.update({data.ticker: order})

    def beg_of_period(self):
        """
        `Beginning of period PNL Accounting`

        Update curr cash and curr equity.
        Consider both open positions and
        cash flows from rateslike assets.

        Carry is assumed to be in %return
        for the timeframe of the main index

        E.g. If we're dealing with Brazilian
        CDI. Say it's currently at 10% y.y.
        Therefore it's daily carry is given
        by: carry = (1+10%) ^(1/252) -1

        """

        self.cash[0] = self.last_cash
        self.open[0] = self.last_equity

        for pos in self.position_stack:
            data = pos.data
            mult = data.multiplier

            MTM = pos.size * (data.open[0] - data.close[-1]) * mult

            self.open[0] += MTM
            if not data.stocklike:
                self.cash[0] += MTM

            ## When cash is consumed, it cannot yield carry ##
            ## Rateslike assets are swap-like against carry ##
            if data.cashlike:
                if self.last_carry is not None:
                    dollar_expo = pos.size * mult * data.close[-1]
                    self.cash[0] -= dollar_expo * self.last_carry
                else:
                    warn("Carry values were not inputed..")

        for order in self.order_stack:
            if order.status == _STATUS["WAIT"]:
                self.__execute_order(order)
            if order.status in (_STATUS["EXEC"], _STATUS["CANC"]):
                self.__orders.pop(order.data.ticker)

    def __execute_order(self, order: Order):
        """
        `Order Execution`

        - Checks whether price is executable.
        - Checks whether cash is sufficient.
        - Updates Open Equity and Open Cash.
        - Updates Positions and create/closes them if necessary.

        OBS: Even though executed price might occur in-between
        open-close times, we apply a simplyfing assumption that
        it occurs exactly at open.

        When order is `valid`:

        - Update cash: Commission + Securities $$ reqs.
        - Update equity: Commission + Mark-to-Market.

        OBS: Mark-to-market is given by the equation:
        M2M = - (Size) * (Exec - Open) * (Multiplier)

        where order is buy if size > 0, and sell otherwise.

        If order.exec_price is None, it means that the
        limit price is not feasible for execution.

        """
        if order.exec_price is None:
            return

        data = order.data
        mult = data.multiplier

        total_comm = order.total_comm
        CASH = M2M = total_comm

        if order.data.stocklike:
            CASH += order.size * order.exec_price

        if self.cash[0] < CASH:
            msg = f"{order} requires too much cash!"
            raise ValueError(msg)
        else:
            self.cash[0] -= CASH

        M2M = order.size * (order.exec_price - data.open[0]) * mult
        if self.open[0] < M2M:
            msg = f"{order} makes equity below zero!"
            raise ValueError(msg)
        else:
            self.open[0] -= M2M

        if not data.ticker in self.__positions:
            position = Position(data=data, size=order.size)
            self.__positions.update({data.ticker: position})

        else:
            position = self.__positions[data.ticker]
            position.add(delta=order.size)
            if not position.size:
                self.__positions.pop(data.ticker)

        order.exec_date = self.date
        order.status = _STATUS["EXEC"]
        self.__executed.append(order)

    def end_of_period(self):
        """
        `End of period PNL Accounting`

        Update curr cash and curr equity
        Consider only closing positions.

        OBS: Since we have already taken into account the
        trading mark-to-market effect of the executed [new]
        positions at the execution stage, we don't need to
        separate the new positions from the old ones at this
        stage.

        E.g. Suppose we had 10 shares of ABC at $10 and we
        make a new trade at $11, so that we have now 100 ABC
        shares. Data Summary:

        old_qty = 10, trade_qty = 90, new_qty = 100

        last_close = 10
        open_price = 10.5
        exec_price = 11
        curr_close = 12

        PNL = 90*(12-11) + 10 *(12-10), right?

        But we're actually doing this in three stages:

        1) Beg of period (or BoP): 10*(10.5 - 10)
        2) Execution             : 90*(10.5 - 11)
        3) End of period (or EoP): 100*(12 - 10.5)

        See these two PNL account methods match!

        """

        self.equity[0] = self.open[0]

        for pos in self.position_stack:
            data, mult = pos.data, pos.data.multiplier
            MTM = pos.size * (data.close[0] - data.open[0]) * mult
            self.equity[0] += MTM
            if not data.stocklike:
                self.cash[0] += MTM

    def __cancel_order(self, order: Order):
        if order.status == _STATUS["WAIT"]:
            print(f"Order cancelled: {order}")

        order.status = _STATUS["CANC"]
        self.__cancels.append(order)

    def __repr__(self):
        kls = self.__class__.__name__
        return f"{kls}(Cash: {self.curr_cash}, Equity: {self.curr_equity})"

    def __getitem__(self, line: str):
        return self.__lines.get(line.lower())

    def __getattr__(self, line: str):
        return self.__lines.get(line.lower())

    def __len__(self):
        return len(self.__index)

    def next(self):
        self.__buffer += 1
        for line in self.__lines.values():
            line._Line__next()

    def get_position(self, ticker: str) -> Optional[Position]:
        return self.__positions.get(ticker)

    def get_orders(self, ticker: str) -> Optional[Position]:
        return self.__orders.get(ticker)

    @property
    def curr_cash(self) -> Number:
        return self.__lines["cash"][0]

    @property
    def curr_equity(self) -> Number:
        return self.__lines["equity"][0]

    @property
    def curr_open(self) -> Number:
        return self.__lines["open"][0]

    @property
    def last_cash(self) -> Number:
        return self.__lines["cash"][-1]

    @property
    def last_equity(self) -> Number:
        return self.__lines["equity"][-1]

    @property
    def last_open(self) -> Number:
        return self.__lines["open"][-1]

    @property
    def carry(self) -> Optional[float]:
        if not hasattr(self, "carry"):
            return
        return self.carry.close[0]

    @property
    def last_carry(self) -> Optional[float]:
        if not hasattr(self, "carry"):
            return
        return self.carry.close[-1]

    @property
    def positions(self) -> Dict[str, Position]:
        return self.__positions

    @property
    def orders(self) -> Dict[str, Order]:
        return self.__orders

    @property
    def order_stack(self) -> List[Order]:
        if not self.__orders:
            return []
        return list(self.__orders.values())

    @property
    def position_stack(self) -> List[Position]:
        if not self.__positions:
            return []
        return list(self.__positions.values())

    @property
    def index(self) -> Line:
        return self.__lines["__index"]

    @property
    def date(self) -> str:
        return self.index[0].isoformat()

    @property
    def lines(self) -> Sequence[str]:
        return list(self.__lines.keys())
