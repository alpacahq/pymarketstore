import json
import msgpack
import requests
from typing import Dict, Union


class MsgpackRpcClient(object):
    codec = msgpack
    mimetype = "application/x-msgpack"

    def __init__(self, endpoint: str):
        if not endpoint:
            raise ValueError('The `endpoint` parameter is required')

        self._id = 1
        self._endpoint = endpoint
        self._session = requests.Session()

    def __getattr__(self, method: str):
        assert self._endpoint is not None

        def call(**kwargs):
            return self._session.post(
                self._endpoint,
                data=self.codec.dumps(self.request(method, **kwargs)),
                headers={"Content-Type": self.mimetype})

        return call

    def call(self, rpc_method: str, **query):
        reply = self._rpc_request(rpc_method, **query)
        return self._rpc_response(reply)

    def _rpc_request(self, method: str, **query) -> Union[Dict, requests.Response]:
        http_resp = self._session.post(
            self._endpoint,
            data=self.codec.dumps(dict(
                method=method,
                id=str(self._id),
                jsonrpc='2.0',
                params=query,
            )),
            headers={"Content-Type": self.mimetype}
        )

        # compat with unittest.mock
        if (not isinstance(requests.Response, type)
                or not isinstance(http_resp, requests.Response)):
            return http_resp

        http_resp.raise_for_status()
        return self.codec.loads(http_resp.content, encoding='utf-8')

    @staticmethod
    def _rpc_response(reply: Dict) -> str:
        error = reply.get('error', None)
        if error:
            raise Exception('{}: {}'.format(error['message'],
                                            error.get('data', '')))

        if 'result' in reply:
            return reply['result']

        raise Exception('invalid JSON-RPC protocol: missing error or result key')
