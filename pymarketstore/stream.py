import msgpack
import re
import websocket
from websocket import ABNF


class StreamConn(object):

    def __init__(self, endpoint):
        self.endpoint = endpoint

        self._handlers = {}

    def _connect(self):
        ws = websocket.WebSocket()
        ws.connect(self.endpoint)
        return ws

    def _subscribe(self, ws, streams):
        msg = msgpack.dumps({
            'streams': streams,
        })
        ws.send(msg, opcode=ABNF.OPCODE_BINARY)

    def run(self, streams):
        ws = self._connect()
        try:
            self._subscribe(ws, streams)
            while True:
                r = ws.recv()
                msg = msgpack.loads(r, encoding='utf-8')
                key = msg.get('key')
                if key is not None:
                    self._dispatch(key, msg)
        finally:
            ws.close()

    def _dispatch(self, stream, msg):
        for pat, handler in self._handlers.items():
            if pat.match(stream):
                handler(self, msg)

    def on(self, stream_pat):
        def decorator(func):
            self.register(stream_pat, func)
            return func

        return decorator

    def register(self, stream_pat, func):
        if isinstance(stream_pat, str):
            stream_pat = re.compile(stream_pat)
        self._handlers[stream_pat] = func

    def deregister(self, stream_pat):
        if isinstance(stream_pat, str):
            stream_pat = re.compile(stream_pat)
        del self._handlers[stream_pat]
