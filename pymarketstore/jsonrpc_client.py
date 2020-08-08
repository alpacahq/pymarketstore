import logging
import numpy as np
import pandas as pd
import re
import requests

from typing import Union, Dict, List

from .jsonrpc import MsgpackRpcClient
from .params import Params, ListSymbolsFormat
from .results import QueryReply
from .stream import StreamConn
from .utils import is_iterable, timeseries_data_to_write_request

logger = logging.getLogger(__name__)


class JsonRpcClient(object):

    def __init__(self, endpoint: str = 'http://localhost:5993/rpc'):
        self.endpoint = endpoint
        self.rpc = MsgpackRpcClient(self.endpoint)

    def _request(self, method: str, **query) -> Dict:
        try:
            return self.rpc.call(method, **query)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def query(self, params: Union[Params, List[Params]]) -> QueryReply:
        if not is_iterable(params):
            params = [params]

        reply = self._request('DataService.Query', requests=[
            p.to_query_request() for p in params
        ])
        return QueryReply.from_response(reply)

    def write(self, data: Union[pd.DataFrame, pd.Series, np.ndarray, np.recarray],
              tbk: str,
              isvariablelength: bool = False,
              ) -> dict:
        dataset = timeseries_data_to_write_request(data, tbk)
        return self.rpc.call("DataService.Write", requests=[dict(
            dataset=dict(
                types=dataset['column_types'],
                names=dataset['column_names'],
                data=dataset['column_data'],
                startindex={tbk: 0},
                lengths={tbk: len(data)},
            ),
            is_variable_length=isvariablelength,
        )])

    def list_symbols(self, fmt: ListSymbolsFormat = ListSymbolsFormat.SYMBOL) -> List[str]:
        reply = self._request('DataService.ListSymbols', format=fmt.value)
        return reply.get('Results') or []

    def destroy(self, tbk: str) -> Dict:
        """
        Delete a bucket
        :param tbk: Time Bucket Key Name (i.e. "TEST/1Min/Tick" )
        :return: reply object
        """
        destroy_req = {'requests': [{'key': tbk}]}
        reply = self._request('DataService.Destroy', **destroy_req)
        return reply

    def server_version(self) -> str:
        resp = requests.head(self.endpoint)
        return resp.headers.get('Marketstore-Version')

    def stream(self):
        endpoint = re.sub('^http', 'ws',
                          re.sub(r'/rpc$', '/ws', self.endpoint))
        return StreamConn(endpoint)

    def __repr__(self):
        return 'MsgPackRPCClient("{}")'.format(self.endpoint)
