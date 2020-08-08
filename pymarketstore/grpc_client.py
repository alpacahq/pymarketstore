import grpc
import logging
import numpy as np
import pandas as pd

from typing import List, Union

from .params import Params, ListSymbolsFormat
from .proto import marketstore_pb2 as proto
from .proto import marketstore_pb2_grpc as gp
from .results import QueryReply
from .utils import is_iterable, timeseries_data_to_write_request

logger = logging.getLogger(__name__)


class GRPCClient(object):

    def __init__(self, endpoint: str = 'localhost:5995'):
        self.endpoint = endpoint
        # set max message sizes
        options = [
            ('grpc.max_send_message_length', 1 * 1024 ** 3),  # 1GB
            ('grpc.max_receive_message_length', 1 * 1024 ** 3),  # 1GB
        ]
        self.channel = grpc.insecure_channel(endpoint, options=options)
        self.stub = gp.MarketstoreStub(self.channel)

    def query(self, params: Union[Params, List[Params]]) -> QueryReply:
        if not is_iterable(params):
            params = [params]

        reply = self.stub.Query(self._build_query(params))
        return QueryReply.from_grpc_response(reply)

    def write(self, data: Union[pd.DataFrame, pd.Series, np.ndarray, np.recarray],
              tbk: str,
              isvariablelength: bool = False,
              ) -> proto.MultiServerResponse:
        req = proto.MultiWriteRequest(requests=[dict(
            data=dict(
                data=timeseries_data_to_write_request(data, tbk),
                start_index={tbk: 0},
                lengths={tbk: len(data)},
            ),
            is_variable_length=isvariablelength,
        )])
        return self.stub.Write(req)

    def _build_query(self, params: Union[Params, List[Params]]) -> proto.MultiQueryRequest:
        if not is_iterable(params):
            params = [params]

        return proto.MultiQueryRequest(requests=[p.to_query_request() for p in params])

    def list_symbols(self, fmt: ListSymbolsFormat = ListSymbolsFormat.SYMBOL) -> List[str]:
        if fmt == ListSymbolsFormat.TBK:
            req_format = proto.ListSymbolsRequest.Format.TIME_BUCKET_KEY
        else:
            req_format = proto.ListSymbolsRequest.Format.SYMBOL

        resp = self.stub.ListSymbols(proto.ListSymbolsRequest(format=req_format))

        if resp is None:
            return []
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
