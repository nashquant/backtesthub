#! /usr/bin/env python3

## CARTESIAN PRODUCT
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
_DEFAULT_CASH: float = float(os.getenv("DEF_CASH", "10e6"))
_DEFAULT_BUFFER: int = int(os.getenv("DEF_BUFFER", "200"))
_DEFAULT_STEP: int = int(os.getenv("DEF_STEP", "1"))
_DEFAULT_SDATE: date = eval(os.getenv("DEF_SDATE", "date(2005,1,1)"))
_DEFAULT_EDATE: date = eval(os.getenv("DEF_EDATE", "date.today()"))

_MODE = dict(
    V="VECTORIZED",
    R="RECURSIVE",
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

_PAIRS = [
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
    S="PERC",
    F="ABS",
)

_METHOD = dict(
    V="VOLATILITY",
    F="FIXED",
)

_HMETHOD = dict(
    E="EXPOSITION",
    B="BETA",
)

_PMETHOD = dict(
    DEFT="DEFAULT",
    RANK="RANKING",
    ROLL="ROLLING",
    VERT="VERTICE",
)

_RATESLIKE = (
    "DI1",
    "DAP",
    "DDI",
)

_OTYPE = {
    "M": "MARKET",
    "L": "LIMIT",
}

_STATUS = dict(
    W="WAITING",
    E="EXECUTED",
    C="CANCELLED",
)
