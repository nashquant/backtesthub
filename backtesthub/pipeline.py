#! /usr/bin/env python3
from typing import Dict

from .utils.bases import Asset
from .utils.config import _PMETHOD


class Pipeline:
    
    def __init__(
        self,
        assets: Dict[str, Asset] = {},
        pmethod: str = "R",
    ):

        if pmethod not in _PMETHOD:
            msg = "Pipeline Method not implemented"
            raise NotImplementedError(msg)

        for tk, asset in assets.items():
            pass

    def run(self) -> Dict[str, Asset]:
        pass

    def __repr__(self):
        log = ""

        return log
