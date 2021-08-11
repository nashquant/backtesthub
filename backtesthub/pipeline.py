#! /usr/bin/env python3
from typing import Dict

from .utils.types import Asset


class Pipeline:
    
    def __init__(
        self,
        assets: Dict[str, Asset] = {},
        method: str = "RANK",
    ):

        for tk, asset in assets.items():
            pass

    def run(self) -> Dict[str, Asset]:
        pass

    def __repr__(self):
        log = ""

        return log
