import numpy as np
import pandas as pd
import six

from collections import defaultdict


def decode(packed):

    dt = np.dtype([
        (colname if isinstance(colname, str) else colname.encode("utf-8"),
         coltype if isinstance(colname, str) else coltype.encode("utf-8"))
        for colname, coltype in zip(packed['names'], packed['types'])
    ])

    array = np.empty((packed['length'],), dtype=dt)
    for idx, name in enumerate(dt.names):
        array[name] = np.frombuffer(packed['data'][idx], dtype=dt[idx])
    return array


def decode_responses(responses):
    results = []
    for response in responses:
        packed = response['result']
        array_dict = {}
        array = decode(packed)
        for tbk, start_idx in six.iteritems(packed['startindex']):
            length = packed['lengths'][tbk]
            array_dict[tbk] = array[start_idx:start_idx + length]
        results.append(array_dict)
    return results


class DataSet(object):

    def __init__(self, array, key, timezone):
        self.array = array
        self._key = key
        self.tbk = dict(zip(key.split(':')[1].split('/'),
                            key.split(':')[0].split('/')))
        self.timezone = timezone

    @property
    def symbol(self):
        return self.tbk['Symbol']

    @property
    def timeframe(self):
        return self.tbk['Timeframe']

    @property
    def attribute_group(self):
        return self.tbk['AttributeGroup']

    def df(self):
        idxname = self.array.dtype.names[0]
        df = pd.DataFrame(self.array).set_index(idxname)
        index = pd.to_datetime(df.index, unit='s', utc=True)
        tz = self.timezone
        if tz.lower() != 'utc':
            index = index.tz_convert(tz)
        df.index = index
        return df

    def __repr__(self):
        a = self.array
        return 'DataSet(key={}, shape={}, dtype={})'.format(
            self._key, a.shape, a.dtype,
        )


class QueryReply(object):

    def __init__(self, reply):
        self._reply = reply
        self._datasets = {
            tbk: DataSet(array, tbk, reply['timezone'])
            for r in decode_responses(reply['responses'])
            for tbk, array in six.iteritems(r)
        }
        self._symbols = defaultdict(dict)
        self._timeframes = defaultdict(dict)
        for ds in self._datasets.values():
            self._symbols[ds.symbol][ds.timeframe] = ds
            self._timeframes[ds.timeframe][ds.symbol] = ds

        # this if/else is needed for compat with unittest.mock
        self._default_timeframe = (list(self._timeframes.keys())[0]
                                   if self._timeframes else None)

    @property
    def timezone(self):
        return self._reply['timezone']

    def all(self):
        return self._datasets

    def first(self):
        return self._datasets[list(self._datasets.keys())[0]]

    def latest_df(self, timeframe=None):
        """
        Collapse the most recent bars from each queried symbol into a single
        DataFrame indexed by symbol with the queried attribute groups as the
        columns (eg OHLCV -> (Open, High, Low, Close, Volume))
        """
        timeframe = timeframe or self._default_timeframe
        columns = self.first().array.dtype.names
        symbols = []
        latest_bars = []

        for ds in self._timeframes[timeframe].values():
            symbols.append(ds.symbol)
            latest_bars.append(ds.array[-1].tolist())

        df = pd.DataFrame(latest_bars, index=symbols, columns=columns)
        if 'Epoch' in columns:
            df.Epoch = pd.to_datetime(df.Epoch, unit='s', utc=True)
        return df

    def keys(self):
        return list(self._datasets.keys())

    def symbols(self):
        return list(self._symbols.keys())

    def by_symbols(self, timeframe=None):
        timeframe = timeframe or self._default_timeframe
        return {symbol: data[timeframe]
                for symbol, data in six.iteritems(self._symbols)}

    def timeframes(self):
        return list(self._timeframes.keys())

    def by_timeframes(self):
        return self._timeframes

    def __getitem__(self, key):
        if key in self._datasets:
            return self._datasets[key]
        elif key in self._timeframes:
            return self._timeframes[key]
        elif key in self._symbols:
            return self._symbols[key]
        raise KeyError('unrecognized TimeBucketKey, timeframe, or symbol: {}'
                       ''.format(key))

    def __contains__(self, key):
        return (key in self._datasets
                or key in self._timeframes
                or key in self._symbols)

    def __iter__(self):
        return iter(self._datasets.values())

    def __str__(self):
        dataset_strings = ',\n'.join(str(ds) for ds in self._datasets.values())
        return "<QueryReply datasets=[{}]>".format(dataset_strings)

    def __repr__(self):
        return 'QueryReply(reply={!r})'.format(self._reply)
