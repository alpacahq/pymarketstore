import logging
import numpy as np
import re

from typing import List, Dict, Union

from .grpc_client import GRPCClient
from .jsonrpc_client import JsonRpcClient
from .params import Params
from .results import QueryReply

logger = logging.getLogger(__name__)

http_regex = re.compile(r'^https?://(.+):\d+/rpc')  # http:// or https://


class Client:
    def __init__(self, endpoint: str = 'http://localhost:5993/rpc', grpc: bool = False):
        self.endpoint = endpoint
        if not grpc:
            self.client = JsonRpcClient(self.endpoint)
            return

        # when endpoint is specified in "http://{host}:{port}/rpc" format,
        # extract the host and initialize GRPC client with default port(5995) for compatibility
        match = re.findall(http_regex, endpoint)
        if match:
            host = match[0] if match[0] != "" else "localhost"  # default host is "localhost"
            self.endpoint = "{}:5995".format(host)  # default port is 5995
        self.client = GRPCClient(self.endpoint)

    def query(self, params: Union[Params, List[Params]]) -> QueryReply:
        """
        execute QUERY to MarketStore server
        :param params: Params object used to query
        :return: QueryReply object
        """
        return self.client.query(params)

    def write(self, recarray: np.array, tbk: str, isvariablelength: bool = False) -> str:
        """
        execute WRITE to MarketStore server
        :param recarray: numpy.array object to write
        :param tbk: Time Bucket Key string.
        ('{symbol name}/{time frame}/{attribute group name}' ex. 'TSLA/1Min/OHLCV' , 'AAPL/1Min/TICK' )
        :param isvariablelength: should be set true if the record content is variable-length array
        :return:
        """
        return self.client.write(recarray, tbk, isvariablelength=isvariablelength)

    def list_symbols(self) -> List[str]:
        return self.client.list_symbols()

    def destroy(self, tbk: str) -> Dict:
        return self.client.destroy(tbk)

    def server_version(self) -> str:
        return self.client.server_version()

    def __repr__(self):
        return self.client.__repr__()
