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


def timeseries_data_to_write_request(data: Union[pd.DataFrame, pd.Series, np.ndarray, np.recarray],
                                     tbk: str,
                                     ) -> dict:
    if isinstance(data, (np.ndarray, np.recarray)):
        return _np_array_to_dataset_params(data)
    elif isinstance(data, pd.Series):
        return _pd_series_to_dataset_params(data, tbk)
    elif isinstance(data, pd.DataFrame):
        return _pd_dataframe_to_dataset_params(data)
    raise TypeError('data must be pd.DataFrame, pd.Series, np.ndarray, or np.recarray')


def _np_array_to_dataset_params(data: Union[np.ndarray, np.recarray]) -> dict:
    if not data.dtype.names:
        raise TypeError('numpy arrays must declare named column dtypes')

    return dict(column_types=[data.dtype[name].str.replace('<', '')
                              for name in data.dtype.names],
                column_names=list(data.dtype.names),
                column_data=[bytes(memoryview(data[name]))
                             for name in data.dtype.names],
                length=len(data))


def _pd_series_to_dataset_params(data: pd.Series, tbk: str) -> dict:
    # single column of data (indexed by timestamp, eg from ohlcv_df['ColName'])
    if data.index.name == 'Epoch':
        epoch = bytes(memoryview(data.index.to_numpy(dtype='i8') // 10**9))
        return dict(column_types=['i8', data.dtype.str.replace('<', '')],
                    column_names=['Epoch', data.name or tbk.split('/')[-1]],
                    column_data=[epoch, bytes(memoryview(data.to_numpy()))],
                    length=len(data))

    # single row of data (named indexes for one timestamp, eg from ohlcv_df.iloc[N])
    epoch = bytes(memoryview(data.name.to_numpy().astype(dtype='i8') // 10**9))
    return dict(column_types=['i8'] + [data.dtype.str.replace('<', '')
                                       for _ in range(0, len(data))],
                column_names=['Epoch'] + data.index.to_list(),
                column_data=[epoch] + [bytes(memoryview(val)) for val in data.array],
                length=1)


def _pd_dataframe_to_dataset_params(data: pd.DataFrame) -> dict:
    epoch = bytes(memoryview(data.index.to_numpy(dtype='i8') // 10**9))
    return dict(column_types=['i8'] + [dtype.str.replace('<', '')
                                       for dtype in data.dtypes],
                column_names=['Epoch'] + data.columns.to_list(),
                column_data=[epoch] + [bytes(memoryview(data[col].to_numpy()))
                                       for col in data.columns],
                length=len(data))
