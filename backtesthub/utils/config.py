#! /usr/bin/env python3

_META = dict()

_MATURITIES = dict(
    F = "jan", 
    G = "feb",
    H = "mar",
    J = "apr",
    K = "may", 
    M = "jun", 
    N = "jul", 
    Q = "aug", 
    U = "sep", 
    V = "oct", 
    X = "nov", 
    Z = "dec",
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
