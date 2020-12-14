from __future__ import absolute_import

import logging
import re
from typing import Any
from typing import Union, Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

from .jsonrpc import MsgpackRpcClient
from .params import Params, ListSymbolsFormat
from .results import QueryReply
from .stream import StreamConn

logger = logging.getLogger(__name__)


def isiterable(something: Any) -> bool:
    return isinstance(something, (list, tuple, set))


def get_timestamp(value: Union[int, str]) -> pd.Timestamp:
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


class JsonRpcClient(object):

    def __init__(self, endpoint: str = 'http://localhost:5993/rpc', ):
        self.endpoint = endpoint
        self.rpc = MsgpackRpcClient(self.endpoint)

    def _request(self, method: str, **query) -> Dict:
        try:
            return self.rpc.call(method, **query)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def query(self, params: Params) -> QueryReply:
        if not isiterable(params):
            params = [params]
        query = self.build_query(params)
        reply = self._request('DataService.Query', **query)
        return QueryReply.from_response(reply)

    def create(self, tbk: str, dtype: List[Tuple[str, str]], isvariablelength: bool = False) -> dict:
        # dtype: e.g. [('Epoch', 'i8'), ('Ask', 'f4')]
        req = {
            "key": "{}:Symbol/Timeframe/AttributeGroup".format(tbk),
            # e.g. ["Epoch", "Open", "High", "Low", "Close"]
            "column_names": [name for name, type in dtype],
            # e.g. ["i8", "f4", "f4", "f4", "f4"]
            "column_types": [type.replace('<', '').replace('|', '') for name, type in dtype],
            "row_type": "variable" if isvariablelength else "fixed",
        }

        writer = {'requests': [req]}
        try:
            return self.rpc.call("DataService.Create", **writer)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                "Could not contact server")

    def write(self, recarray: np.array, tbk: str, isvariablelength: bool = False) -> str:
        data = {}
        data['types'] = [
            recarray.dtype[name].str.replace('<', '').replace('|', '')
            for name in recarray.dtype.names
        ]
        data['names'] = recarray.dtype.names
        data['data'] = [
            bytes(memoryview(recarray[name]))
            for name in recarray.dtype.names
        ]
        data['length'] = len(recarray)
        data['startindex'] = {tbk: 0}
        data['lengths'] = {tbk: len(recarray)}
        write_request = {}
        write_request['dataset'] = data
        write_request['is_variable_length'] = isvariablelength
        writer = {}
        writer['requests'] = [write_request]

        try:
            return self.rpc.call("DataService.Write", **writer)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                "Could not contact server")

    def build_query(self, params: Union[Params, List[Params]]) -> Dict:
        reqs = []
        if not isiterable(params):
            params = [params]
        for param in params:
            req = {
                'destination': param.tbk,
            }
            if param.key_category is not None:
                req['key_category'] = param.key_category
            if param.start is not None:
                req['epoch_start'], start_nanosec = divmod(param.start.value, 10 ** 9)

                # support nanosec
                if start_nanosec != 0:
                    req['epoch_start_nanos'] = start_nanosec

            if param.end is not None:
                req['epoch_end'], end_nanosec = divmod(param.end.value, 10 ** 9)

                # support nanosec
                if end_nanosec != 0:
                    req['epoch_end_nanos'] = end_nanosec

            if param.limit is not None:
                req['limit_record_count'] = int(param.limit)
            if param.limit_from_start is not None:
                req['limit_from_start'] = bool(param.limit_from_start)
            if param.functions is not None:
                req['functions'] = param.functions
            reqs.append(req)
        return {
            'requests': reqs,
        }

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
