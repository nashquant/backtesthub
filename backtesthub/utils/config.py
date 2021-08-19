#! /usr/bin/env python3

import sys, os
import itertools
import dotenv

from datetime import date

dotenv_path = os.path.join(
    os.path.dirname(__file__),
    ".env",
)
dotenv.load_dotenv(dotenv_path)

_DEFAULT_CURRENCY: str = str(os.getenv("DEF_CURRENCY", "BRL"))
_DEFAULT_MARGIN: float = float(os.getenv("DEF_MARGIN", "0"))
_DEFAULT_SLIPPAGE: float = float(os.getenv("DEF_SLIP", "2e-04"))
_DEFAULT_SCOMMISSION: float = float(os.getenv("DEF_SCOMM", "10e-04"))
_DEFAULT_FCOMMISSION: float = float(os.getenv("DEF_FCOMM", "10"))
_DEFAULT_VOLATILITY: float = float(os.getenv("DEF_VOL", "0.1"))
_DEFAULT_CASH: float = float(os.getenv("DEF_CASH", "10e6"))
_DEFAULT_BUFFER: int = int(os.getenv("DEF_BUFFER", "200"))
_DEFAULT_STEP: int = int(os.getenv("DEF_STEP", "1"))
_DEFAULT_SDATE: date = eval(os.getenv("DEF_SDATE", "date(2005,1,1)"))
_DEFAULT_EDATE: date = eval(os.getenv("DEF_EDATE", "date.today()"))
_DEFAULT_THRESH: float = float(os.getenv("DEF_THRESH", "0.2"))
_DEFAULT_VPARAM: float = float(os.getenv("DEF_VPARAM", "0.05"))


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
    SIZE="SIZE",
    EXPO = "EXPO",
    EWMA = "EWMA",
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