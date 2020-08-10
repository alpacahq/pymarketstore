import numpy as np
import pandas as pd

from typing import Any, Union


def get_timestamp(value: Union[int, str]) -> Union[pd.Timestamp, None]:
    if value is None or isinstance(value, pd.Timestamp):
        return value
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


def is_iterable(something: Any) -> bool:
    """
    check if something is a list, tuple or set
    :param something: any object
    :return: bool. true if something is a list, tuple or set
    """
    return isinstance(something, (list, tuple, set))
