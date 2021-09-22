#! /usr/bin/env python3

from .config import _MATURITIES


def derive_asset(fticker: str) -> str:

    """
    Assumes Standart {Asset|Maturity} ticker
    format for futures where Asset is a 2-3
    letter string, and  maturity can be broken
    down into a single maturity letter - the
    month - plus a two-digit number - the year.

    e.g. WINZ20: {
        Asset: "WIN",
        Maturity: {
            "Month": "Z", ## December
            "Year": "20", ## 2020
        }
    }

    """
    m = fticker[-3]  ## Future Maturity Month
    y = fticker[-2:]  ## Future Maturity Year

    if (
        m not in _MATURITIES
        or not y.isdigit()
    ):
        return fticker

    return fticker[:-3]
