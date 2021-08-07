#! /usr/bin/env python3

from datetime import date, datetime
from numbers import Number
from dataclasses import dataclass, InitVar, field
from warnings import filterwarnings

filterwarnings('ignore')

@dataclass(frozen=True)
class Order:
    
    __ticker: str = field(repr = True, compare = True)
    __limit: Number = field(repr = True, compare = False)
    __size: Number = field(repr = True,  compare = False)

    def __init__(self, **kwargs):

        self.__ticker = kwargs.get('ticker', '')
        self.__limit = kwargs.get('limit', 0)
        self.__size = kwargs.get('size', 0)

        idt = kwargs.get('idt') ## issue date

        if not self.__ticker:
            msg = "You must assign a ticker to the order"
            raise ValueError(msg)

        if isinstance(idt, datetime):
            self.__idt = idt.date()

        elif isinstance(idt, str):
            msg = "Isoformat data reading not ready!"
            raise NotImplementedError(msg)
        
        elif not isinstance(idt, date):
            msg = "`idt` must be date/datetime"
            raise TypeError(msg)


    @property
    def idt(self) -> date:
        
        """
        
        """
        return self.__idt
    
    @property
    def size(self) -> Number:
        
        """
        
        """
        return self.__size
