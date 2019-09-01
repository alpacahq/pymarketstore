from __future__ import absolute_import
import numpy as np
import pandas as pd
import re
import requests
import logging
import six

from .jsonrpc import JsonRpcClient, MsgpackRpcClient
from .results import QueryReply
from .stream import StreamConn

logger = logging.getLogger(__name__)


if six.PY2:
    memoryview = lambda array: buffer(np.ascontiguousarray(array))


data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}


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
        if not isiterable(symbols):
            symbols = [symbols]
        self.tbk = ','.join(symbols) + "/" + timeframe + "/" + attrgroup
        self.key_category = None  # server default
        self.start = get_timestamp(start)
        self.end = get_timestamp(end)
        self.limit = limit
        self.limit_from_start = limit_from_start
        self.functions = None

    def set(self, key, val):
        if not hasattr(self, key):
            raise AttributeError()
        if key in ('start', 'end'):
            setattr(self, key, get_timestamp(val))
        else:
            setattr(self, key, val)
        return self

    def __repr__(self):
        content = ('tbk={}, start={}, end={}, '.format(
            self.tbk, self.start, self.end,
        ) +
            'limit={}, '.format(self.limit) +
            'limit_from_start={}'.format(self.limit_from_start))
        return 'Params({})'.format(content)


class Client(object):

    def __init__(self, endpoint='http://localhost:5993/rpc'):
        self.endpoint = endpoint
        rpc_client = get_rpc_client('msgpack')
        self.rpc = rpc_client(self.endpoint)

    def _request(self, method, **query):
        try:
            resp = self.rpc.call(method, **query)
            resp.raise_for_status()
            rpc_reply = self.rpc.codec.loads(resp.content, encoding='utf-8')
            return self.rpc.response(rpc_reply)
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def query(self, params):
        if not isiterable(params):
            params = [params]
        query = self.build_query(params)
        reply = self._request('DataService.Query', **query)
        return QueryReply(reply)

    def write(self, data, tbk, isvariablelength=False):
        if not isinstance(data, (np.ndarray, np.recarray, pd.Series, pd.DataFrame)):
            raise TypeError('The `data` parameter must be an instance of '
                            'np.ndarray, np.recarry, pd.Series, or pd.DataFrame')

        try:
            reply = self.rpc.call("DataService.Write", requests=[
                _make_write_request(data, tbk, isvariablelength),
            ])
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                "Could not contact server")
        reply_obj = self.rpc.codec.loads(reply.content, encoding='utf-8')
        resp = self.rpc.response(reply_obj)
        return resp

    def build_query(self, params):
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
                req['epoch_start'] = int(param.start.value / (10 ** 9))
            if param.end is not None:
                req['epoch_end'] = int(param.end.value / (10 ** 9))
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

    def list_symbols(self):
        reply = self._request('DataService.ListSymbols')
        if 'Results' in reply.keys():
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


def _make_write_request(data, tbk, isvariablelength=False):
    dataset = dict(length=len(data),
                   startindex={tbk: 0},
                   lengths={tbk: len(data)})

    if isinstance(data, (np.ndarray, np.recarray)):
        dataset.update(_np_array_to_dataset_params(data))
    elif isinstance(data, pd.Series):
        dataset.update(_pd_series_to_dataset_params(data, tbk))
    elif isinstance(data, pd.DataFrame):
        dataset.update(_pd_dataframe_to_dataset_params(data))

    return dict(dataset=dataset, is_variable_length=isvariablelength)


def _np_array_to_dataset_params(array):
    return dict(types=[array.dtype[name].str.replace('<', '')
                       for name in array.dtype.names],
                names=list(array.dtype.names),
                data=[bytes(memoryview(array[name]))
                      for name in array.dtype.names])


def _pd_series_to_dataset_params(series, tbk):
    # one row (timestamp) with multiple columns of data (ie named indexes in the array)
    if isinstance(series.index[0], str):
        epoch = series.name.to_datetime64().astype(dtype='i8') / 10 ** 9
        return dict(types=['i8'] + [series.dtype.str.replace('<', '')
                                    for _ in range(0, len(series))],
                    names=['Epoch'] + series.index.to_list(),
                    data=[bytes(memoryview(epoch.astype('i8')))] + [
                        bytes(memoryview(val)) for val in series.array])

    # many rows (timestamps) of data for the same column
    else:
        epoch = series.index.to_numpy(dtype='i8') / 10 ** 9
        return dict(types=['i8', series.dtype.str.replace('<', '')],
                    names=['Epoch', series.name or tbk.split('/')[-1]],
                    data=[bytes(memoryview(epoch.astype('i8'))),
                          bytes(memoryview(series.to_numpy()))])


def _pd_dataframe_to_dataset_params(df):
    # new_types = df.dtypes.map({
    #     np.dtype(np.float64): np.float32,
    #     np.dtype(np.int64): np.int32,
    # }).to_dict()
    # df = df.astype(new_types)
    epoch = df.index.to_numpy(dtype='i8') / 10 ** 9
    return dict(types=['i8'] + [dtype.str.replace('<', '')
                                for dtype in df.dtypes],
                names=['Epoch'] + df.columns.to_list(),
                data=[bytes(memoryview(epoch.astype('i8')))] + [
                    bytes(memoryview(df[name].to_numpy()))
                    for name in df.columns])
