#! /usr/bin/env python3

from datetime import date, datetime
from typing import Optional, Sequence
from pandas import bdate_range

from .utils.config import _DEFAULT_SDATE, _DEFAULT_EDATE

class Calendar:

    def __init__(
        self,
        start: date = _DEFAULT_SDATE,
        end: date = _DEFAULT_EDATE,
        holidays: Sequence[date] = [], 
    ):
        self.__sdate = start
        self.__edate = end

        if isinstance(self.__sdate, datetime):
            self.__sdate = self.__sdate.date()

        if not isinstance(self.__sdate, date):
            msg = "Arg `sdate` must be a date"
            raise TypeError(msg)

        if isinstance(self.__edate, datetime):
            self.__edate = self.__edate.date()

        if not isinstance(self.__edate, date):
            msg = "Arg `edate` must be a date"
            raise TypeError(msg)

        if not isinstance(self.__holidays, Sequence):
            msg = "Arg `holidays` must be a Sequence"
            raise TypeError(msg)

        if not all(isinstance(dt, date) for dt in self.__holidays):
            msg = "Sequence `holidays` must have date elements"
            raise TypeError(msg)

        self.__index: Sequence[date] = bdate_range(
            start=self.__sdate,
            end=self.__edate,
            holidays=self.__holidays,
        )

    @property
    def index(self) -> Sequence[date]:
        return tuple(self.__index.date)
        


    
