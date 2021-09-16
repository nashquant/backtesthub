#! /usr/bin/env python3

from holidays import BR, US
from datetime import date, datetime
from typing import Sequence
from pandas import bdate_range

from .utils.config import (
    _DEFAULT_SDATE, 
    _DEFAULT_EDATE,
    _DEFAULT_COUNTRY,
)

class Calendar:

    """
    `Calendar Class`

    Class responsible for defining the global index that
    will be applied for all data structures (Lines, Data,
    Broker, etc.)

    It is assumed that if no holidays sequence is given, 
    it will derive the global index from a sequence of
    business days from `start` until `end` dates using
    `holidays` library to derive holidays. In this case,
    It will assume by default that the "holidays set" is 
    determined by a country [set by _DEF_COUNTRY, that 
    is an environment variable], which is by default BR. 
    
    For US and BR, it is assumed that regional calendars 
    are important, bc the most important exchanges are 
    located at specific cities, and those are closed 
    during local holidays.
     
    """

    def __init__(
        self,
        start: date = _DEFAULT_SDATE,
        end: date = _DEFAULT_EDATE,
        holidays: Sequence[date] = [],
        country: str = _DEFAULT_COUNTRY, 
    ):
        self.__sdate = start
        self.__edate = end
        
        if not holidays:
            years = [y for y in range(start.year, end.year+20)]
            if country.upper() in ["BR", "BRAZIL"]:
                calendar = BR(state='SP', years = years)
            elif country.upper() in ["US", "USA", "UNITED STATES"]:
                calendar = US(state='NY', years = years)
        
        self.__holidays = tuple(calendar.keys())

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

        weekmask = "Mon Tue Wed Thu Fri"

        self.__index: Sequence[date] = bdate_range(
            start=self.__sdate,
            end=self.__edate,
            holidays=self.__holidays,
            weekmask=weekmask,
            freq='C',
        )

    @property
    def index(self) -> Sequence[date]:
        return tuple(self.__index.date)
    
    @property
    def holidays(self) -> Sequence[date]:
        return tuple(self.__holidays)
        


    
