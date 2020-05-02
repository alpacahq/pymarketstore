from __future__ import absolute_import
import numpy as np
import pandas as pd
import re
import requests
import logging
import six

from .results import QueryReply
from .grpc_client import GRPCClient
from .jsonrpc_client import JsonRpcClient
from .stream import StreamConn

logger = logging.getLogger(__name__)

data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}

http_regex = re.compile(r'^(?:http)s?://(.*):\d+/rpc')  # http:// or https://


def isiterable(something):
    return isinstance(something, (list, tuple, set))


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


class Client:
    def __init__(self, endpoint='http://localhost:5993/rpc', grpc=False):
        if grpc:
            match = re.findall(http_regex, endpoint)

            # when endpoint is specified in "http://{host}:{port}/rpc" format,
            # extract the host and initialize GRPC client with default port(5995) for compatibility
            if len(match) != 0:
                host = match[0] if match[0] is not "" else "localhost"  # default host is "localhost"
                self.endpoint = "{}:5995".format(host)  # default port is 5995
                self.client = GRPCClient(self.endpoint)
                return
            else:
                self.endpoint = endpoint
                self.client = GRPCClient(self.endpoint)
                return

        self.endpoint = endpoint
        self.client = JsonRpcClient(self.endpoint)

    def query(self, params):
        return self.client.query(params)

    def build_query(self, params):
        return self.client.build_query(params)

    def write(self, recarray, tbk, isvariablelength=False):
        return self.client.write(recarray, tbk, isvariablelength=False)

    def list_symbols(self):
        return self.client.list_symbols()

    def destroy(self, tbk):
        return self.client.destroy(tbk)

    def server_version(self):
        return self.client.server_version()

    def __repr__(self):
        return self.client.__repr__()
