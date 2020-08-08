import grpc
import logging
import numpy as np

from typing import List, Union

from .params import Params
from .proto import marketstore_pb2 as proto
from .proto import marketstore_pb2_grpc as gp
from .results import QueryReply
from .utils import is_iterable

logger = logging.getLogger(__name__)


class GRPCClient(object):

    def __init__(self, endpoint: str = 'localhost:5995'):
        self.endpoint = endpoint
        self.channel = grpc.insecure_channel(endpoint)
        self.stub = gp.MarketstoreStub(self.channel)

    def query(self, params: Union[Params, List[Params]]) -> QueryReply:
        if not is_iterable(params):
            params = [params]

        reply = self.stub.Query(self._build_query(params))
        return QueryReply.from_grpc_response(reply)

    def write(self, recarray: np.array, tbk: str, isvariablelength: bool = False) -> proto.MultiServerResponse:
        types = [
            recarray.dtype[name].str.replace('<', '')
            for name in recarray.dtype.names
        ]
        names = recarray.dtype.names
        data = [
            bytes(memoryview(recarray[name]))
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

    def _build_query(self, params: Union[Params, List[Params]]) -> proto.MultiQueryRequest:
        if not is_iterable(params):
            params = [params]

        return proto.MultiQueryRequest(requests=[p.to_query_request() for p in params])

    def list_symbols(self) -> List[str]:
        resp = self.stub.ListSymbols(proto.ListSymbolsRequest(
            format=proto.ListSymbolsRequest.Format.TIME_BUCKET_KEY)
        )
        return resp.results

    def destroy(self, tbk: str) -> proto.MultiServerResponse:
        """
        Delete a bucket
        :param tbk: Time Bucket Key Name (i.e. "TEST/1Min/Tick" )
        """
        req = proto.MultiKeyRequest(requests=[proto.KeyRequest(key=tbk)])
        return self.stub.Destroy(req)

    def server_version(self) -> str:
        resp = self.stub.ServerVersion(proto.ServerVersionRequest())
        return resp.version

    def __repr__(self):
        return 'GRPCClient("{}")'.format(self.endpoint)
