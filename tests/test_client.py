from unittest.mock import MagicMock
import pytest
from ast import literal_eval
import numpy as np
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import imp

import pymarketstore as pymkts
from pymarketstore.exceptions import SymbolsError
imp.reload(pymkts.client)


def test_init():
    p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
    tbk = "TSLA/1Min/OHLCV"
    assert p.tbk == tbk


def test_client_init():
    c = pymkts.Client("http://127.0.0.1:5994/rpc")
    assert c.endpoint == "http://127.0.0.1:5994/rpc"
    assert isinstance(c.rpc, pymkts.client.MsgpackRpcClient)


@patch('pymarketstore.client.MsgpackRpcClient')
def test_query(MsgpackRpcClient):
    c = pymkts.Client()
    p = pymkts.Params('BTC', '1Min', 'OHLCV')

    with pytest.raises(SymbolsError):
        c.query(p)

    assert MsgpackRpcClient().call.called == 1


testdata1 = literal_eval(r"""
{'responses': [{'result': {'data': [b'\xf4\xe8^Z\x00\x00\x00\x000\xe9^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00',
     b'{\x14\xaeG\x01\xf4\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@',
     b'{\x14\xaeG\x01\xf4\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\xfe\xc5@',
     b'\x85\xebQ\xb8^\xe0\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@{\x14\xaeG\x01\xfa\xc5@',
     b'H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\xfe\xfd\xc5@',
     b'iL\xd2F\xbf\xaf\n@\xfe\xe6\xff49\xfd\x0b@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xd7\xd2\x8a\x0c\xfe\x00\xf9?'],
    'length': 5,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'UTC',
 'version': 'dev'}
""")  # noqa: E501


@patch('pymarketstore.client.MsgpackRpcClient')
def test_query_exception(MsgpackRpcClient):
    c = pymkts.Client()

    c._request = MagicMock(return_value=testdata1)

    p = pymkts.Params('BTC', '1Min', 'OHLCV')
    c.query(p)

    p = [pymkts.Params('BTC', '1Min', 'OHLCV')]
    c.query(p)

    p = pymkts.Params('unknow', '1Min', 'OHLCV')
    with pytest.raises(SymbolsError):
        c.query(p)

    p = pymkts.Params(['BTC', 'unknow'], '1Min', 'OHLCV')
    with pytest.raises(SymbolsError):
        c.query(p)

    p = [pymkts.Params('BTC', '1Min', 'OHLCV'),
         pymkts.Params('UNKNOWN', '1Min', 'OHLCV')]

    with pytest.raises(SymbolsError):
        c.query(p)


@patch('pymarketstore.client.MsgpackRpcClient')
def test_write(MsgpackRpcClient):
    c = pymkts.Client()
    data = np.array([(1, 0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])
    tbk = 'TEST/1Min/TICK'
    c.write(data, tbk)
    assert MsgpackRpcClient().call.called == 1


def test_build_query():
    c = pymkts.Client("127.0.0.1:5994")
    p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
    p2 = pymkts.Params('FORD', '5Min', 'OHLCV', 1000000000, 4294967296)
    query_dict = c.build_query([p, p2])
    test_query_dict = {}
    test_lst = []
    param_dict1 = {
        'destination': 'TSLA/1Min/OHLCV',
        'epoch_start': 1500000000,
        'epoch_end': 4294967296
    }
    test_lst.append(param_dict1)
    param_dict2 = {
        'destination': 'FORD/5Min/OHLCV',
        'epoch_start': 1000000000,
        'epoch_end': 4294967296
    }
    test_lst.append(param_dict2)
    test_query_dict['requests'] = test_lst
    assert query_dict == test_query_dict

    query_dict = c.build_query(p)
    assert query_dict == {'requests': [param_dict1]}


@patch('pymarketstore.client.MsgpackRpcClient')
def test_list_symbols(MsgpackRpcClient):
    c = pymkts.Client()
    c.list_symbols()
    assert MsgpackRpcClient().call.called == 1
