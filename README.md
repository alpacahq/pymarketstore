# pymarketstore
Python driver for MarketStore

Build Status: ![build status](https://circleci.com/gh/alpacahq/pymarketstore/tree/master.png?971fa5b1079e8af0568db6caf772132c54f04dc2)

Pymarketstore can query and write financial timeseries data from [MarketStore](https://github.com/alpacahq/marketstore)

Tested with 2.7, 3.3+

## How to install

```
$ pip install pymarketstore
```

## Examples

```
In [1]: import pymarketstore as pymkts

## query data

In [2]: param = pymkts.Params('BTC', '1Min', 'OHLCV', limit=10)

In [3]: cli = pymkts.Client()

In [4]: reply = cli.query(param)

In [5]: reply.first().df()
Out[5]:
                               Open      High       Low     Close     Volume
Epoch
2018-01-17 17:19:00+00:00  10400.00  10400.25  10315.00  10337.25   7.772154
2018-01-17 17:20:00+00:00  10328.22  10359.00  10328.22  10337.00  14.206040
2018-01-17 17:21:00+00:00  10337.01  10337.01  10180.01  10192.15   7.906481
2018-01-17 17:22:00+00:00  10199.99  10200.00  10129.88  10160.08  28.119562
2018-01-17 17:23:00+00:00  10140.01  10161.00  10115.00  10115.01  11.283704
2018-01-17 17:24:00+00:00  10115.00  10194.99  10102.35  10194.99  10.617131
2018-01-17 17:25:00+00:00  10194.99  10240.00  10194.98  10220.00   8.586766
2018-01-17 17:26:00+00:00  10210.02  10210.02  10101.00  10138.00   6.616969
2018-01-17 17:27:00+00:00  10137.99  10138.00  10108.76  10124.94   9.962978
2018-01-17 17:28:00+00:00  10124.95  10142.39  10124.94  10142.39   2.262249

## write data

In [7]: import numpy as np

In [8]: import pandas as pd

In [9]: data = np.array([(pd.Timestamp('2017-01-01 00:00').value / 10**9, 10.0)], dtype=[('Epoch', 'i8'), ('Ask', 'f4')])

In [10]: cli.write(data, 'TEST/1Min/Tick')
Out[10]: {'responses': None}

In [11]: cli.query(pymkts.Params('TEST', '1Min', 'Tick')).first().df()
Out[11]:
                            Ask
Epoch
2017-01-01 00:00:00+00:00  10.0

```

## Client

`pymkts.Client(endpoint='http://localhost:5993/rpc')`

Construct a client object with endpoint.

## Query

`pymkts.Client#query(symbols, timeframe, attrgroup, start=None, end=None, limit=None, limit_from_start=False)`

You can build parameters using `pymkts.Params`.

- symbols: string for a single symbol or a list of symbol string for multi-symbol query
- timeframe: timeframe string
- attrgroup: attribute group string.  symbols, timeframe and attrgroup compose a bucket key to query in the server
- start: unix epoch second (int), datetime object or timestamp string. The result will include only data timestamped equal to or after this time.
- end: unix epoch second (int), datetime object or timestamp string.  The result will include only data timestamped equal to or before this time.
- limit: the number of records to be returned, counting from either start or end boundary.
- limit_from_start: boolean to indicate `limit` is from the start boundary.  Defaults to False.

Pass one or multiple instances of `Params` to `Client.query()`.  It will return `QueryReply` object which holds internal numpy array data returned from the server.

## Write

`pymkts.Client#write(data, tbk)`

You can write a numpy array to the server via `Client.write()` method.  The data parameter must be numpy's [recarray type](https://docs.scipy.org/doc/numpy-dev/reference/generated/numpy.recarray.html) with
a column named `Epoch` in int64 type at the first column.  `tbk` is the bucket key of the data records.

## List Symbols

`pymkts.Client#list_symbols()`

The list of all symbols stored in the server are returned.

## Server version

`pymkts.Client#server_version()`

Returns a string of Marketstore-Version header from a server response.

## Streaming

If the server supports WebSocket streaming, you can connect to it using
`pymkts.StreamConn` class.  For convenience, you can call `pymkts.Client#stream()` to obtain the instance with the same server
information as REST client.

Once you have this instance, you will set up some event handles by
either `register()` method or `@on()` decorator.  These methods accept
regular expressions to filter which stream to act on.

To actually connect and start receiving the messages from the server,
you will call `run()` with the stream names.  By default, it subscribes
to all by `*/*/*`.

`pymkts.Client#stream()`

Return a `StreamConn` which is a websocket connection to the server.

`pymkts.StreamConn#(endpoint)`

Create a connection instance to the `endpoint` server. The endpoint
string is a full URL with "ws" or "wss" scheme with the port and path.

`pymkts.StreamConn#register(stream_path, func)`
`@pymkts.StreamConn#on(stream_path)`

Add a new message handler to the connection.  The function will be called
with `handler(StreamConn, {"key": "...", "data": {...,}})` if the key
(time bucket key) matches with the `stream_path` regular expression.
The `on` method is a decorator version of `register`.

`pymkts.StreamConn#run([stream1, stream2, ...])`

Start communication with the server and go into an indefinite loop. It
does not return until unhandled exception is raised, in which case the
connection is closed so you need to implement retry.  Also, since this is
a blocking method, you may need to run it in a background thread.


An example code is as follows.

```
import pymarketstore as pymkts

conn = pymkts.StreamConn('ws://localhost:5993/ws')

@conn.on(r'^BTC/')
def on_btc(conn, msg):
    print('received btc', msg['data'])

conn.run(['BTC/*/*'])  # runs until exception

-> received btc {'Open': 4370.0, 'High': 4372.93, 'Low': 4370.0, 'Close': 4371.74, 'Volume': 3.3880948699999993, 'Epoch': 1507299600}
```