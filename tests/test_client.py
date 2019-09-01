import numpy as np
import pandas as pd
import pymarketstore as pymkts
import pytest
import six

if six.PY2:
    from mock import patch
    import imp
    imp.reload(pymkts.client)
else:
    from unittest.mock import patch
    import importlib
    importlib.reload(pymkts.client)


from .test_results import btc_array, btc_bytes,  testdata1_df


class TestParams(object):
    def test_init(self):
        p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
        tbk = "TSLA/1Min/OHLCV"
        assert p.tbk == tbk


class TestClient(object):
    def test_init(self):
        c = pymkts.Client("http://127.0.0.1:5994/rpc")
        assert c.endpoint == "http://127.0.0.1:5994/rpc"
        assert isinstance(c.rpc, pymkts.client.MsgpackRpcClient)

    def test_build_query(self):
        c = pymkts.Client("127.0.0.1:5994")

        p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
        param_dict1 = {
            'destination': 'TSLA/1Min/OHLCV',
            'epoch_start': 1500000000,
            'epoch_end': 4294967296
        }

        p2 = pymkts.Params('FORD', '5Min', 'OHLCV', 1000000000, 4294967296)
        param_dict2 = {
            'destination': 'FORD/5Min/OHLCV',
            'epoch_start': 1000000000,
            'epoch_end': 4294967296
        }

        assert c.build_query([p, p2]) == {'requests': [param_dict1, param_dict2]}
        assert c.build_query(p) == {'requests': [param_dict1]}

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_query(self, MsgpackRpcClient):
        c = pymkts.Client()
        p = pymkts.Params('BTC', '1Min', 'OHLCV')
        c.query(p)
        assert MsgpackRpcClient().call.called == 1

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_write_ndarray(self, MsgpackRpcClient):
        c = pymkts.Client()
        data = np.array([(1, 0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])
        tbk = 'TEST/1Min/TICK'
        c.write(data, tbk)
        assert MsgpackRpcClient().call.called == 1

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_write_recarray(self, MsgpackRpcClient):
        c = pymkts.Client()
        data = np.rec.fromrecords([(1, 0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])
        tbk = 'TEST/1Min/TICK'
        c.write(data, tbk)
        assert MsgpackRpcClient().call.called == 1

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_write_series(self, MsgpackRpcClient):
        c = pymkts.Client()
        data = pd.Series([(1, 0)], dtype='f4')
        tbk = 'TEST/1Min/TICK'
        c.write(data, tbk)
        assert MsgpackRpcClient().call.called == 1

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_write_dataframe(self, MsgpackRpcClient):
        c = pymkts.Client()
        data = pd.DataFrame({'Bid': [1, 2], 'Ask': [1, 2]}, dtype=('f4', 'f4'))
        tbk = 'TEST/1Min/TICK'
        c.write(data, tbk)
        assert MsgpackRpcClient().call.called == 1

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_destroy(self, MsgpackRpcClient):
        c = pymkts.Client()
        tbk = 'TEST/1Min/TICK'
        c.destroy(tbk)
        assert MsgpackRpcClient().call.called == 1

    @patch('pymarketstore.client.MsgpackRpcClient')
    def test_list_symbols(self, MsgpackRpcClient):
        c = pymkts.Client()
        c.list_symbols()
        assert MsgpackRpcClient().call.called == 1


class TestWriteRequestDatasetParams(object):
    def test_np_array(self):
        assert pymkts.client._np_array_to_dataset_params(btc_array) == dict(
            data=btc_bytes,
            names=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
            types=['i8', 'f8', 'f8', 'f8', 'f8', 'f8'],
        )

    def test_pd_series_indexed_by_timestamp(self):
        series = pd.Series(btc_array['Open'], index=btc_array['Epoch'] * 10**9)
        assert pymkts.client._pd_series_to_dataset_params(series, 'Open') == dict(
            data=[btc_bytes[0], btc_bytes[1]],
            names=['Epoch', 'Open'],
            types=['i8', 'f8'],
        )

    def test_pd_series_indexed_by_column_name(self):
        idx = ['Open', 'High', 'Low', 'Close', 'Volume']
        series = pd.Series([btc_array[col][0] for col in idx], index=idx)
        series.name = pd.Timestamp(btc_array['Epoch'][0] * 10**9)
        expected_epoch = series.name.to_datetime64().astype('i8') / 10**9
        assert pymkts.client._pd_series_to_dataset_params(series, 'BTC/1Min/OHLCV') == dict(
            data=[bytes(memoryview(expected_epoch.astype('i8')))] + [
                bytes(memoryview(val)) for val in series.array
            ],
            names=['Epoch'] + idx,
            types=['i8'] + [series.dtype.str.replace('<', '') for _ in range(0, len(series))],
        )

    def test_pd_series_sliced_from_df(self):
        series = testdata1_df.iloc[0]
        expected_epoch = series.name.to_datetime64().astype('i8') / 10**9
        assert pymkts.client._pd_series_to_dataset_params(series, 'BTC/1Min/OHLCV') == dict(
            data=[bytes(memoryview(expected_epoch.astype('i8')))] + [
                bytes(memoryview(val)) for val in series.array
            ],
            names=['Epoch'] + testdata1_df.columns.to_list(),
            types=['i8'] + [series.dtype.str.replace('<', '') for _ in range(0, len(series))],
        )

    def test_pd_dataframe(self):
        df = pd.DataFrame(btc_array).set_index('Epoch')
        df.index = df.index * 10**9
        assert pymkts.client._pd_dataframe_to_dataset_params(df) == dict(
            data=btc_bytes,
            names=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
            types=['i8', 'f8', 'f8', 'f8', 'f8', 'f8'],
        )

    @pytest.mark.parametrize('isvariablelength', [True, False])
    def test_make_write_request(self, isvariablelength):
        tbk = 'BTC/1Min/OHLCV'
        write_request = pymkts.client._make_write_request(btc_array, tbk,
                                                          isvariablelength)
        assert write_request == dict(
            dataset=dict(
                length=5,
                startindex={tbk: 0},
                lengths={tbk: 5},
                data=btc_bytes,
                names=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
                types=['i8', 'f8', 'f8', 'f8', 'f8', 'f8'],
            ),
            is_variable_length=isvariablelength,
        )
