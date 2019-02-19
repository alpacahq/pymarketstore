import json
import msgpack
import requests


class JsonRpcClient(object):

    codec = json
    mimetype = "application/json"

    def __init__(self, endpoint):
        self._id = 1
        self._endpoint = endpoint
        self._session = requests.Session()

    def request(self, rpc_method, **query):
        reply = self._rpc_request(rpc_method, **query)
        return self._rpc_response(reply)

    def _rpc_request(self, method, **query):
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

        # FIXME: compat with unittest.mock
        if not isinstance(requests.Response, type):
            return http_resp

        http_resp.raise_for_status()
        return self.codec.loads(http_resp.content, encoding='utf-8')

    @staticmethod
    def _rpc_response(reply):
        error = reply.get('error', None)
        if error:
            raise Exception('{}: {}'.format(error['message'],
                                            error.get('data', '')))

        if 'result' in reply:
            return reply['result']

        raise Exception('invalid JSON-RPC protocol: missing error or result key')


class MsgpackRpcClient(JsonRpcClient):
    codec = msgpack
    mimetype = "application/x-msgpack"
