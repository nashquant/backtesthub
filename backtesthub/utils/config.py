#! /usr/bin/env python3

import sys, os
import itertools
import dotenv

from datetime import date

dotenv.load_dotenv()

_DEFAULT_MIN_SIZE: int = int(os.getenv("DEF_MIN_SIZE", "1"))
_DEFAULT_CURRENCY: str = str(os.getenv("DEF_CURRENCY", "BRL"))
_DEFAULT_CARRY: str = str(os.getenv("DEF_CARRY", "CARRY"))
_DEFAULT_CRATE: float = float(os.getenv("DEF_CRATE", "1.0000394862194537"))
_DEFAULT_SIZING: str = str(os.getenv("DEF_SIZING", "EWMA"))
_DEFAULT_SLIPPAGE: float = float(os.getenv("DEF_SLIP", "2e-04"))
_DEFAULT_SCOMMISSION: float = float(os.getenv("DEF_SCOMM", "10e-04"))
_DEFAULT_FCOMMISSION: float = float(os.getenv("DEF_FCOMM", "10"))
_DEFAULT_VOLATILITY: float = float(os.getenv("DEF_VOL", "0.1"))
_DEFAULT_CASH: float = float(os.getenv("DEF_CASH", "100e6"))
_DEFAULT_BUFFER: int = int(os.getenv("DEF_BUFFER", "200"))
_DEFAULT_SDATE: date = eval(os.getenv("DEF_SDATE", "date(2005,1,1)"))
_DEFAULT_EDATE: date = eval(os.getenv("DEF_EDATE", "date.today()"))
_DEFAULT_THRESH: float = float(os.getenv("DEF_THRESH", "0.2"))
_DEFAULT_VPARAM: float = float(os.getenv("DEF_VPARAM", "0.05"))
_DEFAULT_LAG: int = int(os.getenv("DEF_LAG", "4"))
_DEFAULT_ECHO: bool = bool(os.getenv("DEF_ECHO", "True"))
_DEFAULT_URL = {
    "drivername": str(os.getenv("_DRIVER","")),
    "username": str(os.getenv("_USER","")),
    "password": str(os.getenv("_PASSWORD","")),
    "host": str(os.getenv("_HOST","")),
    "database": str(os.getenv("_DATABASE","")),
}

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
