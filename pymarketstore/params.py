from typing import Union, List, Any
import pandas as pd
import numpy as np
from enum import Enum


def get_timestamp(value: Union[int, str]) -> pd.Timestamp:
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


def isiterable(something: Any) -> bool:
    """
    check if something is a list, tuple or set
    :param something: any object
    :return: bool. true if something is a list, tuple or set
    """
    return isinstance(something, (list, tuple, set))


class ListSymbolsFormat(Enum):
    """
    format of the list_symbols response.
    """
    # symbol names only. (e.g. ["AAPL", "AMZN", ...])
    SYMBOL = "symbol"
    # {symbol}/{timeframe}/{attribute_group} format. (e.g. ["AAPL/1Min/TICK", "AMZN/1Sec/OHLCV",...])
    TBK = "tbk"


class Params(object):

    def __init__(self, symbols: Union[List[str], str], timeframe: str, attrgroup: str,
                 start: Union[int, str] = None, end: Union[int, str] = None,
                 limit: int = None, limit_from_start: bool = None,
                 columns: List[str] = None):
        if not isiterable(symbols):
            symbols = [symbols]
        self.tbk = ','.join(symbols) + "/" + timeframe + "/" + attrgroup
        self.key_category = None  # server default
        self.start = get_timestamp(start)
        self.end = get_timestamp(end)
        self.limit = limit
        self.limit_from_start = limit_from_start
        self.columns = columns
        self.functions = None

    def set(self, key: str, val: Any):
        if not hasattr(self, key):
            raise AttributeError()
        if key in ('start', 'end'):
            setattr(self, key, get_timestamp(val))
        else:
            setattr(self, key, val)
        return self

    def __repr__(self) -> str:
        content = ('tbk={}, start={}, end={}, '.format(
            self.tbk, self.start, self.end,
        ) +
                   'limit={}, '.format(self.limit) +
                   'limit_from_start={}'.format(self.limit_from_start) +
                   'columns={}'.format(self.columns))
        return 'Params({})'.format(content)
