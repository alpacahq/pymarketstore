import pandas as pd

from pymarketstore.utils import timeseries_data_to_write_request

from .test_results import btc_array, btc_bytes, btc_df


class TestTimeseriesDataToWriteRequest:
    def test_np_array(self):
        assert timeseries_data_to_write_request(btc_array, 'BTC/1Min/OHLCV') == dict(
            column_data=btc_bytes,
            column_names=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
            column_types=['i8', 'f8', 'f8', 'f8', 'f8', 'f8'],
            length=5,
        )

    def test_pd_series_indexed_by_timestamp(self):
        series = pd.Series(btc_df.Open, index=btc_df.index)
        assert timeseries_data_to_write_request(series, 'BTC/1Min/Open') == dict(
            column_data=[btc_bytes[0], btc_bytes[1]],
            column_names=['Epoch', 'Open'],
            column_types=['i8', 'f8'],
            length=5,
        )

    def test_pd_series_row_from_df(self):
        series = btc_df.iloc[0]
        expected_epoch = bytes(memoryview(series.name.to_numpy().astype(dtype='i8') // 10**9))
        assert timeseries_data_to_write_request(series, 'BTC/1Min/OHLCV') == dict(
            column_data=[expected_epoch] + [bytes(memoryview(val)) for val in series.array],
            column_names=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
            column_types=['i8', 'f8', 'f8', 'f8', 'f8', 'f8'],
            length=1,
        )

    def test_pd_dataframe(self):
        assert timeseries_data_to_write_request(btc_df, 'BTC/1Min/OHLCV') == dict(
            column_data=btc_bytes,
            column_names=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
            column_types=['i8', 'f8', 'f8', 'f8', 'f8', 'f8'],
            length=5,
        )
