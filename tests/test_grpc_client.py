import numpy as np

import pymarketstore as pymkts

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from pymarketstore.proto import marketstore_pb2_grpc
from pymarketstore.proto.marketstore_pb2 import MultiQueryRequest, QueryRequest


def test_grpc_client_init():
    c = pymkts.GRPCClient("127.0.0.1:5995")
    assert c.endpoint == "127.0.0.1:5995"
    assert isinstance(c.stub, marketstore_pb2_grpc.MarketstoreStub)


@patch('pymarketstore.grpc_client.marketstore_pb2_grpc.MarketstoreStub')
def test_query(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    p = pymkts.Params('BTC', '1Min', 'OHLCV')

    # --- when ---
    c.query(p)

    # --- then ---
    assert c.stub.Query.called == 1


@patch('pymarketstore.grpc_client.marketstore_pb2_grpc.MarketstoreStub')
def test_write(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    data = np.array([(1, 0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])
    tbk = 'TEST/1Min/TICK'

    # --- when ---
    c.write(data, tbk)

    # --- then ---
    assert c.stub.Write.called == 1


def test_build_query():
    # --- given ---
    c = pymkts.GRPCClient(endpoint="127.0.0.1:5995")
    p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)

    # --- when ---
    query = c.build_query([p])

    # --- then ---
    assert query == MultiQueryRequest(
        requests=[QueryRequest(destination="TSLA/1Min/OHLCV", epoch_start=1500000000, epoch_end=4294967296)])

# Not implemented yet
# @patch('pymarketstore.client.MsgpackRpcClient')
# def test_list_symbols(MsgpackRpcClient):
#     c = pymkts.GRPCClient()
#     c.list_symbols()
#     assert MsgpackRpcClient().call.called == 1
#
#
# @patch('pymarketstore.client.MsgpackRpcClient')
# def test_destroy(MsgpackRpcClient):
#     c = pymkts.GRPCClient()
#     tbk = 'TEST/1Min/TICK'
#     c.destroy(tbk)
#     assert MsgpackRpcClient().call.called == 1
