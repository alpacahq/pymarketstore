import numpy as np


def df_to_recarray(df):
    recarray = df.to_records(index_dtypes='i8')
    recarray.Epoch = recarray.Epoch / 10 ** 9
    return recarray


def series_to_recarray(series, col_name=None):
    epoch = series.index.to_numpy(dtype='i8') / 10**9
    return np.rec.fromrecords(
        list(zip(epoch, series.to_numpy())),
        dtype=['i8', series.dtype.str.replace('<', '')],
        names=['Epoch', col_name or series.name],
    )
