from __future__ import absolute_import

import logging

import grpc
import requests
import six

import pymarketstore.proto.marketstore_pb2 as proto
import pymarketstore.proto.marketstore_pb2_grpc as gp
from .results import QueryReply

logger = logging.getLogger(__name__)


def isiterable(something):
    return isinstance(something, (list, tuple, set))


class GRPCClient(object):

    def __init__(self, endpoint='localhost:5995'):
        self.endpoint = endpoint
        self.channel = grpc.insecure_channel(endpoint)
        self.stub = gp.MarketstoreStub(self.channel)

    def query(self, params):
        if not isiterable(params):
            params = [params]
        reqs = self.build_query(params)

        reply = self.stub.Query(reqs)

        return QueryReply.from_grpc_response(reply)

    def write(self, recarray, tbk, isvariablelength=False):
        types = [
            recarray.dtype[name].str.replace('<', '')
            for name in recarray.dtype.names
        ]
        names = recarray.dtype.names
        data = [
            bytes(buffer(recarray[name])) if six.PY2
            else bytes(memoryview(recarray[name]))
            for name in recarray.dtype.names
        ]
        length = len(recarray)
        start_index = {tbk: 0}
        lengths = {tbk: len(recarray)}

        req = proto.MultiWriteRequest(requests=[
            proto.WriteRequest(
                data=proto.NumpyMultiDataset(
                    data=proto.NumpyDataset(
                        column_types=types,
                        column_names=names,
                        column_data=data,
                        length=length,
                        # data_shapes = [],
                    ),
                    start_index=start_index,
                    lengths=lengths,
                ),
                is_variable_length=isvariablelength,
            )
        ])

        return self.stub.Write(req)

    def build_query(self, params):
        reqs = proto.MultiQueryRequest(requests=[])
        if not isiterable(params):
            params = [params]
        for param in params:
            req = proto.QueryRequest(
                destination=param.tbk,
            )

            if param.key_category is not None:
                req.key_category = param.key_category
            if param.start is not None:
                req.epoch_start = int(param.start.value / (10 ** 9))

                # support nanosec
                # start_nanosec = int(param.start.value % (10 ** 9))
                # if start_nanosec != 0:
                #    req.epoch_start_nanos = start_nanosec

            if param.end is not None:
                req.epoch_end = int(param.end.value / (10 ** 9))

                # support nanosec
                # end_nanosec = int(param.end.value % (10 ** 9))
                # if end_nanosec != 0:
                #    req.epoch_end_nanos = end_nanosec

            if param.end is not None:
                req.epoch_end = int(param.end.value / (10 ** 9))
            if param.limit is not None:
                req.limit_record_count = int(param.limit)
            if param.limit_from_start is not None:
                req.limit_from_start = bool(param.limit_from_start)
            if param.functions is not None:
                req.functions = param.functions
            reqs.requests.append(req)
        return reqs

    def list_symbols(self):
        resp = self.stub.ListSymbols(proto.ListSymbolsRequest())
        return resp.results

    def destroy(self, tbk):
        """
        Delete a bucket
        :param tbk: Time Bucket Key Name (i.e. "TEST/1Min/Tick" )
        """
        req = proto.MultiKeyRequest(requests=[proto.KeyRequest(key=tbk)])
        return self.stub.Destroy(req)

    def server_version(self):
        resp = self.stub.ServerVersion(proto.ServerVersionRequest())
        return resp.version

    def __repr__(self):
        return 'GRPCClient("{}")'.format(self.endpoint)
