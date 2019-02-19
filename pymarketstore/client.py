from __future__ import absolute_import
import numpy as np
import pandas as pd
import re
import requests
import logging
import six

from ._compat import getfullargspec
from .jsonrpc import JsonRpcClient, MsgpackRpcClient
from .results import QueryReply
from .stream import StreamConn

logger = logging.getLogger(__name__)


DATA_TYPE_CONV = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}
TIMEFRAME_RE = re.compile(r'^([0-9]+)(Sec|Min|H|D|W|M|Y)$')


def isiterable(something):
    return isinstance(something, (list, tuple, set))


def get_rpc_client(codec='msgpack'):
    if codec == 'msgpack':
        return MsgpackRpcClient
    return JsonRpcClient


def get_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


class Params(object):

    def __init__(self, symbols, timeframe, attrgroup,
                 start=None, end=None,
                 limit=None, limit_from_start=None):
        self.symbols = symbols
        self.timeframe = timeframe
        self.attrgroup = attrgroup
        self._key_category = 'Symbol/Timeframe/AttributeGroup'

        self.start = start
        self.end = end
        self.limit = limit
        self.limit_from_start = limit_from_start
        self.functions = None

    @property
    def tbk(self):
        return '/'.join([
            ','.join(self.symbols),
            self.timeframe,
            self.attrgroup,
        ])

    @property
    def symbols(self):
        return self._symbols

    @symbols.setter
    def symbols(self, symbols):
        self._symbols = symbols if isiterable(symbols) else [symbols]

    @property
    def timeframe(self):
        return self._timeframe

    @timeframe.setter
    def timeframe(self, timeframe):
        if not TIMEFRAME_RE.match(timeframe):
            raise ValueError('Timeframe must be in the format of '
                             '^([0-9]+)(Sec|Min|H|D|W|M|Y)$')

        self._timeframe = timeframe

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, start):
        self._start = get_timestamp(start)

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, end):
        self._end = get_timestamp(end)

    def set(self, key, val):
        if key not in getfullargspec(self.__init__).args[1:]:
            raise AttributeError(key)

        setattr(self, key, val)
        return self

    def to_rpc(self):
        query = dict(
            destination=self.tbk,
            key_category=self._key_category,
            epoch_start=(None if self.start is None
                         else int(self.start.value / 10**9)),
            epoch_end=(None if self.end is None
                       else int(self.end.value / 10**9)),
            limit_record_count=(None if self.limit is None
                                else int(self.limit)),
            limit_from_start=(None if self.limit_from_start is None
                              else bool(self.limit_from_start)),
            functions=self.functions
        )
        return {k: v for k, v in six.iteritems(query) if v is not None}

    def __repr__(self):
        return (
            'Params('
                'symbols={!r}, timeframe={!r}, attrgroup={!r}, '
                'start={!r}, end={!r}, '
                'limit={!r}, limit_from_start={!r}'
            ')'.format(
                self.symbols, self.timeframe, self.attrgroup,
                self.start, self.end,
                self.limit, self.limit_from_start
            ))


class Client(object):

    def __init__(self, endpoint='http://localhost:5993/rpc'):
        self.endpoint = endpoint
        rpc_client = get_rpc_client('msgpack')
        self.rpc = rpc_client(self.endpoint)

    def _request(self, method, **query):
        try:
            return self.rpc.request(method, **query)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise exc

    def query(self, params):
        params = params if isiterable(params) else [params]
        reply = self._request('DataService.Query',
                              requests=[p.to_rpc() for p in params])
        return QueryReply(reply)

    def write(self, recarray, tbk, is_variable_length=False):
        data = {}
        data['types'] = [
            recarray.dtype[name].str.replace('<', '')
            for name in recarray.dtype.names
        ]
        data['names'] = recarray.dtype.names
        data['data'] = [
            bytes(buffer(recarray[name])) if six.PY2
                else bytes(memoryview(recarray[name]))
            for name in recarray.dtype.names
        ]
        data['length'] = len(recarray)
        data['startindex'] = {tbk: 0}
        data['lengths'] = {tbk: len(recarray)}

        write_request = {}
        write_request['dataset'] = data
        write_request['is_variable_length'] = is_variable_length

        return self.rpc.request("DataService.Write", requests=[write_request])

    def list_symbols(self):
        reply = self._request('DataService.ListSymbols')
        if 'Results' in reply:
            return reply['Results']
        return []

    def server_version(self):
        resp = requests.head(self.endpoint)
        return resp.headers.get('Marketstore-Version')

    def stream(self):
        endpoint = re.sub('^http', 'ws',
                          re.sub(r'/rpc$', '/ws', self.endpoint))
        return StreamConn(endpoint)

    def __repr__(self):
        return 'Client("{}")'.format(self.endpoint)
