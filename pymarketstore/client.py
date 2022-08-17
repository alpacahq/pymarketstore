from __future__ import absolute_import

import logging
import re
from typing import List, Dict, Union, Tuple, Any

import numpy as np

from .jsonrpc_client import JsonRpcClient
from .params import Params, ListSymbolsFormat
from .results import QueryReply

logger = logging.getLogger(__name__)

data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}

http_regex = re.compile(r'^https?://(.+):\d+/rpc')  # http:// or https://


class RequestError(Exception):
    "Generic client error"


def err_on_resp(response: dict) -> None:
    """
    Raise any errors found in responses from client request.
    """
    responses = response['responses']
    if responses is not None:
        for r in responses:
            err = r['error']
            if err:
                raise RequestError(err)


class Client:
    def __init__(
        self,
        endpoint: str = 'http://localhost:5993/rpc',
        grpc: bool = False,
        raise_errors: bool = False,

    ) -> None:
        self._raise_errors = raise_errors
        if grpc:
            from .grpc_client import GRPCClient
            match = re.findall(http_regex, endpoint)

            # when endpoint is specified in "http://{host}:{port}/rpc" format,
            # extract the host and initialize GRPC client with default port(5995) for compatibility
            if len(match) != 0:
                host = match[0] if match[0] != "" else "localhost"  # default host is "localhost"
                self.endpoint = "{}:5995".format(host)  # default port is 5995
                self.client = GRPCClient(self.endpoint)
                return
            else:
                self.endpoint = endpoint
                self.client = GRPCClient(self.endpoint)
                return

        self.endpoint = endpoint
        self.client = JsonRpcClient(self.endpoint)

    def _maybe_raise(
        self,
        result: Any,
    ) -> Any:
        """
        If an error response is received back from the server, raise a
        local ``RequestError`` with its contents.

        """
        if not self._raise_errors:
            return result
        else:
            return err_on_resp(result)

    def query(self, params: Params) -> QueryReply:
        """
        execute QUERY to MarketStore server
        :param params: Params object used to query
        :return: QueryReply object
        """
        return self._maybe_raise(
            self.client.query(params)
        )

    def sql(self, statements: Union[str, List[str]]) -> QueryReply:
        """
        execute SQL to MarketStore server
        :param statements: List of SQL statements in a string
        :return: QueryReply object
        """
        return self._maybe_raise(
            self.client.sql(statements)
        )

    def _build_query(self, params: Union[Params, List[Params]]) -> Dict:
        return self.client.build_query(params)

    def create(self, tbk: str, dtype: List[Tuple[str, str]], isvariablelength: bool = False):
        """
        create a new bucket
        :param tbk: Time Bucket Key string. (e.g. TSLA/1Min/OHLCV )
        :param  dtype: data shapes of the bucket (e.g. [("Epoch", "i8"), ("Bid", "f4"), ("Ask", "f4")] )
        :param isvariablelength: should be set true if the record content is variable-length array
        :return: str
        """
        return self._maybe_raise(
            self.client.create(tbk=tbk, dtype=dtype, isvariablelength=isvariablelength)
        )

    def write(self, recarray: np.array, tbk: str, isvariablelength: bool = False) -> str:
        """
        execute WRITE to MarketStore server
        :param recarray: numpy.array object to write
        :param tbk: Time Bucket Key string.
        ('{symbol name}/{time frame}/{attribute group name}' ex. 'TSLA/1Min/OHLCV' , 'AAPL/1Min/TICK' )
        :param isvariablelength: should be set true if the record content is variable-length array
        :return:
        """
        return self._maybe_raise(
            self.client.write(recarray, tbk, isvariablelength=isvariablelength)
        )

    def list_symbols(self, fmt: ListSymbolsFormat = ListSymbolsFormat.SYMBOL) -> List[str]:
        return self.client.list_symbols(fmt)

    def destroy(self, tbk: str) -> Dict:
        return self._maybe_raise(
            self.client.destroy(tbk)
        )

    def server_version(self) -> str:
        return self._maybe_raise(
            self.client.server_version()
        )

    def __repr__(self):
        return self.client.__repr__()
