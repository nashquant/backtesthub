import pandas as pd
import numpy as np

from typing import Union
from ..utils.bases import (
    Base,
    Asset,
    Hedge,
)


def Default(
    data: Union[Base, Asset, Hedge],
    *args,
) -> pd.Series:

    return np.ones(len(data))


def SMACross(
    data: Union[Base, Asset, Hedge],
    p1: int,
    p2: int,
    *args,
) -> pd.Series:
    """
    Return simple moving average of `values`,
    at each step taking into account `n`
    previous values.
    """

    sma1 = pd.Series(data.close).rolling(p1).mean()
    sma2 = pd.Series(data.close).rolling(p2).mean()

    return np.sign(sma1 - sma2)
