#! /usr/bin/env python3

import math
import numpy as np
import pandas as pd
from numbers import Number
from datetime import date
from collections import defaultdict as ddict
from typing import Dict, List, Optional, Sequence, Union

from .order import Order
from .position import Position

from .utils.bases import Line, Base, Asset
from .utils.config import (
    _DEFAULT_CURRENCY,
    _DEFAULT_BUFFER,
    _DEFAULT_CRATE,
    _DEFAULT_CASH,
    _STATUS,
)


class Broker:
    def __init__(
        self,
        echo: bool,
        index: Sequence[date],
        cash: Number = _DEFAULT_CASH,
    ):
        self.__echo = echo
        self.__index = index
        self.__startcash = cash
        self.__length = len(index)
        self.__buffer = _DEFAULT_BUFFER
        self.__carry: float = _DEFAULT_CRATE
        self.__market: Optional[float] = None
        self.__positions: Dict[str, Position] = {}
        self.__orders: Dict[str, Order] = {}
        self.__cancels: List[Order] = []
        self.__executed: List[Order] = []
        self.__currs: Dict[str, Base] = {}

        self.__cash = np.ones(self.__length) * cash
        self.__open = np.ones(self.__length) * cash
        self.__equity = np.ones(self.__length) * cash

        self.__opnl: dict[str, Number] = ddict(float)  ## overnight
        self.__ipnl: dict[str, Number] = ddict(float)  ## intraday
        self.__tpnl: dict[str, Number] = ddict(float)  ## trade
        self.__cpnl: dict[str, Number] = ddict(float)  ## carry

        self.__records: Sequence[Union[date, Number]] = list()

    def add_carry(self, carry: Base):
        if not isinstance(carry, Base):
            msg = "Wrong input type for carry"
            raise TypeError(msg)

        self.__carry = carry

    def add_market(self, market: Base):
        if not isinstance(market, Base):
            msg = "Wrong input type for carry"
            raise TypeError(msg)

        self.__market = market

    def add_curr(self, curr: Base):
        if not isinstance(curr, Base):
            msg = "Wrong input type for carry"
            raise TypeError(msg)

        self.__currs.update({curr.ticker: curr})

    def new_order(
        self,
        data: Asset,
        size: Number,
        limit: Optional[Number] = None,
        stop: Optional[Number] = None,
    ):
        """
        `Order Creation`

        - Checks order for the same ticker.
        - If so, cancels that order.
        - Updates Open Orders Dictionary.

        """

        if size == 0 or not isinstance(size, Number):
            return

        ticker = data.ticker
        pending = self.__orders.get(ticker)

        if self.__echo:
            txt = (
                f"Broker<Order Received for "
                f"{ticker}, Size: {size:.2f}, "
                f"Price: {data.close[0]:.2f}, "
                f"Signal: {data.signal[0]:.2f}>"
            )
            print(f"{self.date.isoformat()}, {txt}")

        if pending is not None:
            self.__cancel_order(pending)

        order = Order(data, size, limit, stop)
        self.__orders.update({ticker: order})

        if ticker not in self.__positions:
            self.__positions.update(
                {
                    ticker: Position(
                        data=data,
                        size=0,
                        stop=stop,
                    )
                },
            )

    def close(self, data: Asset):
        ticker = data.ticker
        if not ticker in self.__positions:
            return
        pos = self.__positions.get(ticker)
        self.new_order(pos.data, -pos.size)

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

        Trades in foreign currencies are adjusted
        by the FX's closing price of the day (even
        if at the BoP, theoretically, the price is
        still not available). This is because this
        sep between BoP and EoP is "virtual", i.e.
        it doesn't exist in practice. Actually, the
        total pnl is given by the differences
        between close price and the previous close
        or the execution price, both cases adjusted
        by the close price of the currency.

        """

        self.__opnl.clear(), self.__ipnl.clear()
        self.__cpnl.clear(), self.__tpnl.clear()

        self.__cash[self.__buffer] = self.last_cash
        self.__open[self.__buffer] = self.last_equity
        self.__equity[self.__buffer] = self.last_equity

        for pos in self.position_stack:
            data = pos.data
            ticker = data.ticker
            factor = data.multiplier
            curr = data.currency
            if not curr == _DEFAULT_CURRENCY:
                pair = f"{curr}{_DEFAULT_CURRENCY}"
                factor *= self.__currs[pair].close[0]

            MTM = pos.size * (data.open[0] - data.close[-1]) * factor

            self.__open[self.__buffer] += MTM
            self.__equity[self.__buffer] += MTM
            self.__opnl[ticker] += MTM
            if not data.stocklike:
                self.__cash[self.__buffer] += MTM

            ## When cash is consumed, it cannot yield carry ##
            ## Rateslike assets are swap-like against carry ##
            if data.cashlike:
                dollar_expo = pos.size * factor * data.close[-1]
                carry = -dollar_expo * self.last_carry
                self.__open[self.__buffer] += carry
                self.__equity[self.__buffer] += carry
                self.__cash[self.__buffer] += carry
                self.__cpnl[ticker] += carry

        for order in self.order_stack:
            if order.status == _STATUS["WAIT"]:
                self.__execute_order(order)
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
        exec_price = order.exec_price
        if exec_price is None:
            return

        data = order.data
        size = order.size
        ticker = data.ticker

        factor = data.multiplier
        curr = data.currency
        if not curr == _DEFAULT_CURRENCY:
            pair = f"{curr}{_DEFAULT_CURRENCY}"
            factor *= self.__currs[pair].close[0]

        total_comm = order.total_comm
        self.__tpnl[ticker] += total_comm
        CASH = M2M = total_comm

        if order.data.stocklike:
            CASH -= size * exec_price

        self.__cash[self.__buffer] += CASH

        M2M = order.size * (data.open[0] - exec_price) * factor
        self.__open[self.__buffer] += M2M
        self.__equity[self.__buffer] += M2M
        self.__tpnl[ticker] += M2M

        position = self.__positions[ticker]
        position.add(order.size)
        
        if not position.size:
            self.__positions.pop(ticker)

        order.exec_date = data.date
        order.status = _STATUS["EXEC"]
        self.__executed.append(order)

        if self.__echo:
            txt = (
                f"Broker<Order Executed for "
                f"{ticker}, Size: {size:.2f}, "
                f"Price: {exec_price:.2f}, "
                f"Fee: {total_comm:.2f}>"
            )
            print(f"{self.date.isoformat()}, {txt}")

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

        for pos in self.position_stack:
            data, ticker = pos.data, pos.ticker
            size, factor = pos.size, data.multiplier

            curr = data.currency
            if not curr == _DEFAULT_CURRENCY:
                pair = f"{curr}{_DEFAULT_CURRENCY}"
                factor *= self.__currs[pair].close[0]

            order = self.__orders.get(ticker)
            price, open = data.close[0], data.open[0]

            if order:
                target = size + order.size
            else:
                target = size

            texpo = target * factor * price
            texpo = texpo / self.curr_equity

            MTM = size * (price - open) * factor
            self.__equity[self.__buffer] += MTM
            self.__ipnl[ticker] += MTM
            if not data.stocklike:
                self.__cash[self.__buffer] += MTM

            if not data.rateslike:
                self.__records.append(
                    {
                        "date": self.date.isoformat(),
                        "ticker": ticker,
                        "asset": data.asset,
                        "size": size,
                        "opnl": self.__opnl[ticker],
                        "ipnl": self.__ipnl[ticker],
                        "tpnl": self.__tpnl[ticker],
                        "cpnl": self.__cpnl[ticker],
                        "signal": data.signal[0],
                        "refvol": data.volatility[0],
                        "target": target,
                        "texpo": texpo,
                    }
                )
            else:
                self.__records.append(
                    {
                        "date": self.date.isoformat(),
                        "ticker": ticker,
                        "asset": data.asset,
                        "size": -size,
                        "opnl": self.__opnl[ticker],
                        "ipnl": self.__ipnl[ticker],
                        "tpnl": self.__tpnl[ticker],
                        "cpnl": self.__cpnl[ticker],
                        "signal": -data.signal[0],
                        "refvol": data.volatility[0],
                        "target": -target,
                        "texpo": texpo,
                    }
                )

    def __cancel_order(self, order: Order):
        if order.status == _STATUS["WAIT"]:
            print(f"Order cancelled: {order}")

        order.status = _STATUS["CANC"]
        self.__cancels.append(order)

    def __repr__(self):
        kls = self.__class__.__name__
        return (
            f"{self.date.isoformat()}, {kls}<"
            f"Return: {self.cum_return:.2f}%, "
            f"{str(self.position_stack)[1:-1]}, "
            f"{str(self.order_stack)[1:-1]}> "
        )

    def __len__(self):
        return self.__length

    def next(self):
        self.__buffer += 1

    def get_position(self, ticker: str) -> Optional[Position]:
        return self.__positions.get(ticker)

    def get_orders(self, ticker: str) -> Optional[Position]:
        return self.__orders.get(ticker)

    def get_expo(self) -> Number:
        """
        Get Current Exposition (% Equity)
        Reference price calculated @ CLOSE
        """
        expo = 0
        
        for pos in self.position_stack:
            data, ticker = pos.data, pos.ticker
            size, factor = pos.size, data.multiplier

            curr = data.currency
            if not curr == _DEFAULT_CURRENCY:
                pair = f"{curr}{_DEFAULT_CURRENCY}"
                factor *= self.__currs[pair].close[0]

            expo+= size * factor * data.close[0] / self.curr_equity
        
        return expo

    def get_texpo(self) -> Number:
        """
        Current Target Exposition (% Equity)
        Reference price calculated @ CLOSE
        """
        texpo = 0
        
        for pos in self.position_stack:
            data, ticker = pos.data, pos.ticker
            size, factor = pos.size, data.multiplier

            curr = data.currency
            if not curr == _DEFAULT_CURRENCY:
                pair = f"{curr}{_DEFAULT_CURRENCY}"
                factor *= self.__currs[pair].close[0]

            order = self.__orders.get(ticker)

            if order:
                target = size + order.size
            else:
                target = size

            texpo+=target * factor * data.close[0] / self.curr_equity
        
        return texpo

    def get_beta(self) -> Number:
        """
        Get Current Beta w/ respect to market
        """
        beta =  0

        if self.__market is None:
            txt="Broker Arg `Market` not specified"
            raise ValueError(txt)

        for pos in self.position_stack:
            data, ticker = pos.data, pos.ticker
            size, factor = pos.size, data.multiplier

            curr = data.currency
            if not curr == _DEFAULT_CURRENCY:
                pair = f"{curr}{_DEFAULT_CURRENCY}"
                factor *= self.__currs[pair].close[0]

            if "beta" not in data.lines:
                df = pd.DataFrame.from_records(
                    {
                        "close": data.close.array,
                        "mclose": data.mclose.array,
                        "vol": data.volatility.array,
                        "mvol": self.__market.volatility.array,
                    },
                    index = data.index.array
                )
                df['ret'] = df.close.pct_change()
                df['mret'] = df.mclose.pct_change() 
                df['corrl'] = df.ret.ewm(alpha=0.05).corr(df.mret)
                df['beta'] = df.corrl * df.vol / df.mvol

                data.add_line("beta", Line(df.beta, buffer = data.buffer))

            beta+= data.beta[0] * size * factor * data.close[0] / self.curr_equity

        return beta

    def get_tbeta(self) -> Number:
        """
        Get Target Beta w/ respect to market
        """
        beta =  0

        if self.__market is None:
            txt="Broker Arg `Market` not specified"
            raise ValueError(txt)

        for pos in self.position_stack:
            data, ticker = pos.data, pos.ticker
            size, factor = pos.size, data.multiplier

            curr = data.currency
            if not curr == _DEFAULT_CURRENCY:
                pair = f"{curr}{_DEFAULT_CURRENCY}"
                factor *= self.__currs[pair].close[0]

            order = self.__orders.get(ticker)

            if order:
                target = size + order.size
            else:
                target = size

            if "beta" not in data.lines:
                df = pd.DataFrame.from_records(
                    {
                        "close": data.close.array,
                        "mclose": self.__market.close.array,
                        "vol": data.volatility.array,
                        "mvol": self.__market.volatility.array,
                    },
                    index = data.index.array
                )
                df['ret'] = df.close.pct_change()
                df['mret'] = df.mclose.pct_change() 
                df['corrl'] = df.ret.ewm(alpha=0.05).corr(df.mret)
                df['beta'] = df.corrl * df.vol / df.mvol

                data.add_line("beta", Line(df.beta, buffer = data.buffer))

            beta+= data.beta[0] * target * factor * data.close[0] / self.curr_equity

        return beta

    @property
    def date(self) -> date:
        return self.__index[self.__buffer]

    @property
    def curr_cash(self) -> Number:
        return self.__cash[self.__buffer]

    @property
    def curr_equity(self) -> Number:
        return self.__equity[self.__buffer]

    @property
    def curr_open(self) -> Number:
        return self.__open[self.__buffer]

    @property
    def last_cash(self) -> Number:
        return self.__cash[self.__buffer - 1]

    @property
    def last_equity(self) -> Number:
        return self.__equity[self.__buffer - 1]

    @property
    def last_open(self) -> Number:
        return self.__open[self.__buffer - 1]

    @property
    def carry(self) -> Optional[float]:
        if isinstance(self.__carry, Base):
            return self.__carry.close[0]

        return self.__carry

    @property
    def last_carry(self) -> Optional[float]:
        if isinstance(self.__carry, Base):
            return self.__carry.close[-1]

        return self.__carry

    @property
    def positions(self) -> Dict[str, Position]:
        return self.__positions

    @property
    def orders(self) -> Dict[str, Order]:
        return self.__orders

    @property
    def currs(self) -> Dict[str, Base]:
        return self.__currs

    @property
    def order_stack(self) -> List[Order]:
        return list(self.__orders.values())

    @property
    def position_stack(self) -> List[Position]:
        return list(self.__positions.values())

    @property
    def index(self) -> List[Number]:
        return self.__index[_DEFAULT_BUFFER : self.__buffer + 1]

    @property
    def cash(self) -> List[Number]:
        return self.__cash[_DEFAULT_BUFFER : self.__buffer + 1]

    @property
    def open(self) -> List[Number]:
        return self.__open[_DEFAULT_BUFFER : self.__buffer + 1]

    @property
    def equity(self) -> List[Number]:
        return self.__equity[_DEFAULT_BUFFER : self.__buffer + 1]

    @property
    def quotas(self) -> List[Number]:
        return 1000 * self.equity / self.__startcash

    @property
    def cum_return(self) -> Number:
        return 100 * (self.curr_equity / self.__startcash - 1)

    @property
    def df(self) -> pd.DataFrame:
        dates = [dt.isoformat() for dt in self.index]
        df = pd.DataFrame.from_records(
            {
                "date": dates,
                "cash": self.cash,
                "open": self.open,
                "equity": self.equity,
                "quota": self.quotas,
            }
        )

        df["returns"] = df.equity.pct_change()
        df["returnsln"] = np.log(1 + df.returns)
        df["m12"] = np.exp(df.returnsln.rolling(252).sum()) - 1
        df["volatility"] = math.sqrt(252) * df.returns.rolling(252).std()
        df["sharpe"] = df["m12"] / df["volatility"]
        df["drawdown"] = df["quota"] / df["quota"].cummax() - 1

        return df

    @property
    def rec(self) -> pd.DataFrame:
        return pd.DataFrame.from_records(self.__records)
