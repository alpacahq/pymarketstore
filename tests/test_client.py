import pymarketstore as pymkts
from pymarketstore import jsonrpc
import numpy as np
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import imp
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
    c.query(p)
    assert MsgpackRpcClient().call.called == 1


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
    query_dict = c._build_query([p, p2])
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

    query_dict = c._build_query(p)
    assert query_dict == {'requests': [param_dict1]}


@patch('pymarketstore.client.MsgpackRpcClient')
def test_list_symbols(MsgpackRpcClient):
    c = pymkts.Client()
    c.list_symbols()
    assert MsgpackRpcClient().call.called == 1
