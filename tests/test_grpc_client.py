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


@patch('pymarketstore.proto.marketstore_pb2_grpc.MarketstoreStub')
def test_query(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    p = pymkts.Params('BTC', '1Min', 'OHLCV')

    # --- when ---
    c.query(p)

    # --- then ---
    assert c.stub.Query.called == 1


@patch('pymarketstore.proto.marketstore_pb2_grpc.MarketstoreStub')
def test_create(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    dtype = [('Epoch', 'i8'), ('Bid', 'f4'), ('Ask', 'f4')]
    tbk = 'TEST/1Min/TICK'

    # --- when ---
    c.create(tbk=tbk, dtype=dtype, isvariablelength=False)

    # --- then ---
    assert c.stub.Create.called == 1


@patch('pymarketstore.proto.marketstore_pb2_grpc.MarketstoreStub')
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


@patch('pymarketstore.proto.marketstore_pb2_grpc.MarketstoreStub')
def test_list_symbols(stub):
    # --- given ---
    c = pymkts.GRPCClient()

    # --- when ---
    c.list_symbols()

    # --- then ---
    assert c.stub.ListSymbols.called == 1


@patch('pymarketstore.proto.marketstore_pb2_grpc.MarketstoreStub')
def test_destroy(stub):
    # --- given ---
    c = pymkts.GRPCClient()
    tbk = 'TEST/1Min/TICK'

    # --- when ---
    c.destroy(tbk)

    # --- then ---
    assert c.stub.Destroy.called == 1


@patch('pymarketstore.proto.marketstore_pb2_grpc.MarketstoreStub')
def test_server_version(stub):
    # --- given ---
    c = pymkts.GRPCClient()

    # --- when ---
    c.server_version()

    # --- then ---
    assert c.stub.ServerVersion.called == 1
