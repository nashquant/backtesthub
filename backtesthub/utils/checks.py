from typing import Any, Dict, Sequence
from .config import _MATURITIES


def derive_params(args: Sequence[Any]) -> str:
    """
    Stringify `*args` in order to enable
    a standardized name for indicators.

    E.g. func = SMACross, args = (10, 200)
    Then, name = SMACross(10,200)

    """

    return str(list(args))[1:-1]


def derive_asset(fticker: str) -> str:

    """
    Assumes Standart {Asset|Maturity} ticker 
    format for futures where Asset is a 2-3 
    letter string, and  maturity can be broken 
    down into a single maturity letter representing 
    the month plus a two-digit number representing 
    the year.

    e.g. WINZ20: {
        Asset: "WIN",
        Maturity: {
            "Month": "Z", ## December
            "Year": "20", ## Context -> 2020
        }
    }

    """
    m = fticker[-3]  ## Future Maturity Month
    y = fticker[-2:]  ## Future Maturity Year

    errmsg = "This `ticker` cannot have"\
    " `mult`property because it is stock-like!"

    if m not in _MATURITIES:
        raise ValueError(errmsg)
    
    if y.isdigit():
        raise ValueError(errmsg)

    return fticker[:-3]