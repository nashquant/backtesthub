#! /usr/bin/env python3

import os
import itertools
from datetime import date, datetime
from itertools import product

_DEFAULT_MIN_SIZE: int = int(os.getenv("DEF_MIN_SIZE", "1"))
_DEFAULT_CURRENCY: str = str(os.getenv("DEF_CURRENCY", "BRL"))
_DEFAULT_CARRY: str = str(os.getenv("DEF_CARRY", "CARRY"))
_DEFAULT_CRATE: float = float(os.getenv("DEF_CRATE", "2e-04"))  ## 5%y.y
_DEFAULT_SIZING: str = str(os.getenv("DEF_SIZING", "EWMA"))
_DEFAULT_HEDGE: str = str(os.getenv("DEF_HEDGE", "EXPO"))
_DEFAULT_SLIPPAGE: float = float(os.getenv("DEF_SLIP", "3e-04"))
_DEFAULT_SCOMMISSION: float = float(os.getenv("DEF_SCOMM", "10e-04"))
_DEFAULT_FCOMMISSION: float = float(os.getenv("DEF_FCOMM", "10"))
_DEFAULT_VOLATILITY: float = float(os.getenv("DEF_VOL", "0.1"))
_DEFAULT_PORTFVOLAT: float = float(os.getenv("DEF_PFVOL", "0.3"))
_DEFAULT_CASH: float = float(os.getenv("DEF_CASH", "100e6"))
_DEFAULT_BUFFER: int = int(os.getenv("DEF_BUFFER", "200"))
_DEFAULT_SDATE: date = eval(os.getenv("DEF_SDATE", "date(2005,1,1)"))
_DEFAULT_EDATE: date = eval(os.getenv("DEF_EDATE", "date.today()"))
_DEFAULT_INCEPTION: date = eval(os.getenv("DEF_INC", "date(1900,1,1)"))
_DEFAULT_MATURITY: date = eval(os.getenv("DEF_MAT", "date(2100,1,1)"))
_DEFAULT_STKMINVOL: float = float(eval(os.getenv("DEF_STKMINVOL", "0.1")))
_DEFAULT_STKMAXVOL: float = float(eval(os.getenv("DEF_STKMAXVOL", "3.0")))
_DEFAULT_LIQTHRESH: float = float(eval(os.getenv("DEF_LIQTHRESH", "0.05")))
_DEFAULT_RATESDAY: int = int(eval(os.getenv("DEF_RATESD", "30")))
_DEFAULT_RATESMONTH: int = int(eval(os.getenv("DEF_RATESM", "6")))
_DEFAULT_THRESH: float = float(os.getenv("DEF_THRESH", "0.2"))
_DEFAULT_VPARAM: float = float(os.getenv("DEF_VPARAM", "0.05"))
_DEFAULT_LAG: int = int(os.getenv("DEF_LAG", "3"))
_DEFAULT_ECHO: bool = bool(os.getenv("DEF_ECHO", "True"))
_DEFAULT_MAX_LOSS: float = float(os.getenv("DEF_MAX_LOSS", "-99"))
_DEFAULT_MARKET: str = os.getenv("DEF_MARKET", "IBOV")
_DEFAULT_COUNTRY: str = os.getenv("DEF_COUNTRY", "BR")
_DEFAULT_N: int = int(os.getenv("DEF_N", "30"))
_DEFAULT_URL = {
    "drivername": str(os.getenv("DB_DRIVER", "")),
    "username": str(os.getenv("DB_USER", "")),
    "password": str(os.getenv("DB_PASSWORD", "")),
    "host": str(os.getenv("DB_HOST", "")),
    "database": str(os.getenv("DB_DATABASE", "")),
}

_MIN_VOL = {
    "ES": 0.12,
    "IND": 0.15,
    "DOL": 0.10,
}

_MIN_VOL.update(
    eval(
        os.getenv(
            "_MIN_VOL",
            str({}),
        )
    )
)

_CURR = (
    "BRL",
    "USD",
    "MXN",
    "CLP",
    "ARS",
    "EUR",
    "CAD",
    "CHF",
    "ZAR",
    "AUD",
    "NZD",
    "CNY",
    "JPY",
    "GBP",
    "HKD",
    "TRY",
)

_DEFAULT_PAIRS = [
    f"{cur1}{cur2}"
    for cur1, cur2 in itertools.product(_CURR, _CURR)
    if not cur1 == cur2
]

_MATURITIES = dict(
    F="jan",
    G="feb",
    H="mar",
    J="apr",
    K="may",
    M="jun",
    N="jul",
    Q="aug",
    U="sep",
    V="oct",
    X="nov",
    Z="dec",
)

_SCHEMA = dict(
    OC=["open", "close"],
    OHLC=["open", "high", "low", "close"],
    OHLCV=["open", "high", "low", "close", "volume"],
)

_COMMTYPE = dict(
    PERC="PERC",
    ABS="ABS",
)

_METHOD = dict(
    EXPO="EXPO",
    EWMA="EWMA",
)


_HMETHOD = dict(
    EXPO="EXPOSITION",
    BETA="BETA",
)

_STATUS = dict(
    WAIT="WAITING",
    EXEC="EXECUTED",
    CANC="CANCELLED",
)

_RATESLIKE = (
    "DI1",
    "DAP",
    "DDI",
)

_YRS = list(
    range(_DEFAULT_SDATE.year, _DEFAULT_EDATE.year + 1)
)

_QRS = list(
    ['Q1','Q2','Q3', 'Q4']
)

_YQRS = [
    f'{y}{q}' for y, q in list(product(_YRS,_QRS)) 
]
