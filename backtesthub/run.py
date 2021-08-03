#! /usr/bin/env python3

import warnings
import numpy as np
import pandas as pd
from .utils.static import * 
from warnings import filterwarnings
from abc import ABCMeta, abstractmethod 

from typing import Callable, Dict, List, \
    Optional, Sequence, Tuple, Type, Union

filterwarnings('ignore')