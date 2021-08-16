#! /usr/bin/env python3

from datetime import date
from typing import Dict, Sequence, Optional

from .utils.bases import Asset, Hedge
from .utils.config import _PMETHOD

class Pipeline:

    """
    Pipeline is responsible for identifying 
    the `simulation case` and recursively
    build a sequence called `universe`, which
    provides, for a given date, the list of
    all tradeable assets, for every run() call.

    This enables interesting features such as
    the broadcasting of a base signal to multiple
    assets, rolling operations for futures, etc.

    "Warning": Lots of cases aren't defined yet. 
    We do have support for those:

    1) Base signal + Futures Rolling. - Must have
    one base data, and a sequence of futures-like
    assets. 
    
    2) Rates-like Vertice Trading - Must have a
    sequence of rates-like(thus, futures-like too)
    and a 

    3) Single Stocklike - ...

    4) Multi Stocklike - ...

    5) Portfolios - ...
    
    """
    
    def __init__(
        self,
        assets: Dict[str, Asset] = {},
        hedges: Dict[str, Hedge] = {},
        **case: Dict[str, Optional[bool]],
    ):

        if len(assets) == 1:
            self.__case = _PMETHOD["DEFT"] ## DEFAULT

        
            

    def run(self, dt: date) -> Sequence[str]:
        pass

    def __repr__(self):
        log = ""

        return log
