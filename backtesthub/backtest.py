#! /usr/bin/env python3

import pandas as pd
from numbers import Number
from uuid import uuid3, NAMESPACE_DNS
from collections import OrderedDict
from datetime import date, datetime
from typing import (
    Dict,
    List,
    Sequence,
    Union,
)

from .broker import Broker
from .pipeline import Pipeline
from .strategy import Strategy
from .calendar import Calendar

from .utils.bases import Line, Base, Asset
from .utils.math import fill_OHLC

from .utils.config import (
    _DEFAULT_BUFFER,
    _DEFAULT_HEDGE,
    _DEFAULT_SIZING,
    _DEFAULT_THRESH,
    _DEFAULT_VPARAM,
    _DEFAULT_MAX_LOSS,
    _DEFAULT_VOLATILITY,
    _DEFAULT_CARRY,
    _DEFAULT_PAIRS,
    _DEFAULT_ECHO,
    _DEFAULT_MARKET,
)

class Backtest:

    """
    `Backtest Class`

    Instances of this class are responsible for orchestrating all
    related objects (Strategy, Broker, Position, ...) in order to
    properly run the simulation.

    It is also responsible for manipulating the global index and
    guaranteeing that all Data/Lines/Broker/Strategy are 
    synchronized.

    Lots of features such as intraday operations, multi-calendar
    runs, live trading, etc. are still pending development.

    Some settings may be changed through environment variables
    configuration. Refer to ~/backtesthub/utils/config.py to get 
    more info.
    """

    def __init__(
        self,
        strategy: Strategy,
        pipeline: Pipeline,
        calendar: Calendar,
        **config: str,
    ):
        if not issubclass(strategy, Strategy):
            msg = "Arg `strategy` must be a `Strategy` subclass!"
            raise TypeError(msg)
        if not issubclass(pipeline, Pipeline):
            msg = "Arg `pipeline` must be a `Pipeline` subclass!"
            raise TypeError(msg)
        if not isinstance(calendar, Calendar):
            msg = "Arg `calendar` must be a `Calendar` instance!"
            raise TypeError(msg)

        self.__index: Sequence[date] = calendar.index
        self.__firstdate: date = self.__index[0]
        self.__lastdate: date = self.__index[-1]

        self.__factor: str = config.get("factor")
        self.__market: str = config.get("market")
        self.__asset: str = config.get("asset")
        self.__hedge: str = config.get("hedge")
        self.__base: str = config.get("base")
        self.__hbase: str = config.get("hbase")
        self.__vertices: List[int] = config.get("vertices")
        self.__compensation: float = config.get("compensation", 1)

        self.__main: Line = Line(self.__index)
        self.__bases: Dict[str, Base] = OrderedDict()
        self.__assets: Dict[str, Asset] = OrderedDict()
        self.__hedges: Dict[str, Asset] = OrderedDict()

        self.__broker: Broker = Broker(
            index=self.__index,
            echo=_DEFAULT_ECHO,
        )

        self.__pipeline: Pipeline = pipeline(
            main=self.__main,
            holidays=calendar.holidays,
            broker=self.__broker,
            assets=self.__assets,
        )

        self.__strategy: Strategy = strategy(
            broker=self.__broker,
            pipeline=self.__pipeline,
            bases=self.__bases,
            assets=self.__assets,
            target=self.target
        )

    def config_hedge(
        self,
        pipeline: Pipeline,
        strategy: Strategy,
    ):
        """
        `Configure Hedge Method`

        Works similarly to the strategy configuration
        but this is specifically designed for hedging.

        To properly define a strategy, one needs to build 
        both pipeline and strategy rules, consistent with
        the asset class and hedge properties desired.

        If HStrategy and HPipeline exists they will be
        properly handled by the main event loop.

        """

        if not issubclass(strategy, Strategy):
            msg = "Arg `strategy` must be a `Strategy` subclass!"
            raise TypeError(msg)
        if not issubclass(pipeline, Pipeline):
            msg = "Arg `pipeline` must be a `Pipeline` subclass!"
            raise TypeError(msg)

        self.__hpipeline: Pipeline = pipeline(
            main=self.__main,
            broker=self.__broker,
            assets=self.__hedges,
        )

        self.__hstrategy: Strategy = strategy(
            broker=self.__broker,
            pipeline=self.__hpipeline,
            bases=self.__bases,
            assets=self.__hedges,
            target=self.target,
        )

    def add_base(
        self,
        ticker: str,
        data: pd.DataFrame,
    ):
        """
        `Add Base Method`

        ############## IMPORTANT ###############

        - Main BASE is assumed to be added FIRST.
        - Main HBASE is assumed to be added LAST.

        ########################################

        Bases that can be classified either as
        1) Currency Pairs, 2) Carry (i.e. "risk
        free" cost of carry), 3) Market (i.e.
        market index that can be used to make
        beta regressions), will be assigned
        to broker so that it may use for 
        important calculations.

        Reminder: Bases are data structures fed 
        for any purpose that is not trading. One 
        may want to define a base for things such 
        as signal generation, currency conversion, 
        volatility estimation, etc. 
        
        More info @ backtesthub/utils/bases.py  
        """

        base = Base(
            ticker=ticker,
            data=data,
            index=self.index,
        )

        self.__bases.update(
            {ticker: base},
        )

        if ticker.upper() in _DEFAULT_PAIRS:
            self.__broker.add_curr(base)
        if ticker.upper() == _DEFAULT_CARRY:
            self.__broker.add_carry(base)
        if ticker.upper() == _DEFAULT_MARKET:
            self.__broker.add_market(base)

    def add_asset(
        self,
        ticker: str,
        data: pd.DataFrame,
        **commkwargs: Union[str, Number],
    ):
        """
        `Add Asset Method`

        Add data structures that will be used for
        trading purposes. This does not mean that
        they cannot be used for same purposes of
        'bases', but you need to be aware of the
        drawbacks.

        `fill_OHLC` is applied to `data`, in order
        to guarantee that broker gets an appropriate
        schema for tradeable assets.

        All assets will be accessible by pointer/
        reference by both the `Broker` and the 
        `Pipeline` objects.
        
        More info @ backtesthub/utils/bases.py. 
        """

        asset = Asset(
            data=fill_OHLC(data),
            ticker=ticker,
            index=self.index,
            **commkwargs,
        )

        self.__assets.update(
            {ticker: asset},
        )

    def add_hedge(
        self,
        ticker: str,
        data: pd.DataFrame,
        **commkwargs: Union[str, Number],
    ):
        """
        `Add Hedge Method`

        Same thing as `add_asset` method, except 
        that this asset will be classified as a 
        hedge by the framework.

        This has implications on the accessibility 
        of this asset by the pipeline - will only
        be accessible by the HPipeline.

        All assets will be accessible by pointer/
        reference by both the `Broker` and the 
        `HPipeline` objects.
        """

        hedge = Asset(
            data=fill_OHLC(data),
            ticker=ticker,
            index=self.index,
            **commkwargs,
        )

        self.__hedges.update(
            {ticker: hedge},
        )

    def run(self) -> Dict[str, pd.DataFrame]:
        
        """
        `Main Event Loop Run`
        
        This method is the core function of the
        whole backtest engine, which has the
        following steps:

        1) Make sure assets have been fed to it.

        2) Initialize Pipeline(s) and Strategy(ies).
        
        3) For each day being an element of the
           `global index`, apply the following
           sequence:

            3.1) Update All Data Buffers (Sync);
            3.2) Update Broker's BoP State;
            3.3) Update Pipeline(s) and Strategy(ies);
            3.4) Update Broker's EoP State;
            3.5) Check if stop condition is reached;
            3.6) If not advance date and get back to 3.1;

        4) Return all relevant information in a organized manner.
        
        """

        if not self.__assets:
            return

        self.config_backtest()

        self.__pipeline.init()
        self.__strategy.init()

        if self.__hedges:
            self.__hpipeline.init()
            self.__hstrategy.init()

        while self.dt < self.__lastdate:
            self.__advance_buffers()
            self.__broker.beg_of_period()
            self.__pipeline.next()
            self.__strategy.next()
            if self.__hedges:
                self.__hpipeline.next()
                self.__hstrategy.next()
            self.__broker.end_of_period()

            if self.__broker.cum_return < _DEFAULT_MAX_LOSS:
                break

        dct = {
            "meta": self.__properties,
            "quotas": self.__broker.df,
            "records": self.__broker.rec,
            "broker": self.__broker,
        }

        dct['quotas']['uid'] = self.__uid.hex
        dct['records']['uid'] = self.__uid.hex

        return dct

    def __advance_buffers(self):
        """
        `Advance Buffer Method`

        Advances all line buffers at once,
        guaranteeing synchronized updates.
        """

        self.__main.next()
        self.__broker.next()
        for data in self.datas.values():
            data.next()

    def __repr__(self) -> str:
        if not hasattr(self, "__hash"):
            self.config_backtest()
        return self.__hash

    @property
    def dt(self) -> date:
        return self.__main[0]

    @property
    def bookname(self) -> str:
        """
        `Bookname Property`

        Defines the official book name formula.
        """

        book = f"{self.__factor}-{self.__market}-{self.__asset}"
        mapper = dict(
            EXPO="/",
            BETA="#",
        )
        if self.__hedge:
            book += f"{mapper[_DEFAULT_HEDGE]}{self.__hedge}"

        return book

    @property
    def index(self) -> Sequence[date]:
        return self.__index

    @property
    def target(self) -> float:
        return self.__compensation * _DEFAULT_VOLATILITY

    @property
    def strategy(self) -> Strategy:
        return self.__strategy

    @property
    def base(self) -> Base:
        return tuple(self.__bases.values())[0]

    @property
    def hbase(self) -> Base:
        return tuple(self.__bases.values())[-1]

    @property
    def bases(self) -> Dict[str, Base]:
        return self.__bases

    @property
    def datas(self) -> Dict[str, Union[Base, Asset]]:
        return {**self.__bases, **self.__assets, **self.__hedges}

    def config_backtest(self):
        """
        `Configure Backtest UID`

        Defines the official uid for 
        backtest given input data.
        """

        self.__hash = {
            "factor": self.__factor,
            "market": self.__market,
            "asset": self.__asset,
            "hedge": self.__hedge,
            "base": self.__base,
            "hbase": self.__hbase,
            "vertices": str(self.__vertices),
            "pipeline": self.__pipeline.__class__.__name__,
            "model": self.__strategy.__class__.__name__,
            "params": str(dict(self.__strategy.get_params())),
        }

        self.__uid = uuid3(
            NAMESPACE_DNS,
            str(self.__hash),
        )

        self.__properties = pd.DataFrame.from_records(
            [
                {
                    **self.__hash,
                    "uid": self.__uid.hex,
                    "sdate": self.__firstdate.isoformat(),
                    "edate": self.__lastdate.isoformat(),
                    "updtime": datetime.now().isoformat(),
                    "budget": _DEFAULT_VOLATILITY,
                    "buffer": _DEFAULT_BUFFER,
                    "sizing": _DEFAULT_SIZING,
                    "thresh": _DEFAULT_THRESH,
                    "vparam": _DEFAULT_VPARAM,
                    "bookname": self.bookname,
                    "compensation": self.__compensation,
                }
            ]
        )
