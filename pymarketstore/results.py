from typing import List, Dict

import numpy as np
import pandas as pd
import six

import pymarketstore.proto.marketstore_pb2 as proto


def decode(column_names: List[str], column_types: List[str], column_data, data_length) -> np.ndarray:
    dt = np.dtype([
        (colname if isinstance(colname, str) else colname.encode("utf-8"),
         coltype if isinstance(colname, str) else coltype.encode("utf-8"))
        for colname, coltype in zip(column_names, column_types)
    ])

    array = np.empty((data_length,), dtype=dt)
    for idx, name in enumerate(dt.names):
        array[name] = np.frombuffer(column_data[idx], dtype=dt[idx])
    return array


def decode_responses(responses: List[Dict]) -> List:
    results = []
    for response in responses:
        packed = response['result']
        array_dict = {}
        # array = decode(packed)
        array = decode(packed['names'], packed['types'], packed['data'], packed['length'])
        for tbk, start_idx in six.iteritems(packed['startindex']):
            length = packed['lengths'][tbk]
            key = str(tbk.split(':')[0])
            array_dict[key] = array[start_idx:start_idx + length]
        results.append(array_dict)
    return results


def decode_grpc_responses(responses) -> List[Dict[str, np.ndarray]]:
    results = []
    for response in responses:
        packed = response.result
        array_dict = {}
        array = decode(packed.data.column_names, packed.data.column_types, packed.data.column_data, packed.data.length)
        for tbk, start_idx in six.iteritems(packed.start_index):
            length = packed.lengths[tbk]
            key = str(tbk.split(':')[0])
            array_dict[key] = array[start_idx:start_idx + length]
        results.append(array_dict)
    return results


class DataSet(object):

    def __init__(self, array: np.ndarray, key: str, timezone: str):
        self.array = array
        self.key = key
        self.timezone = timezone

    @property
    def symbol(self) -> str:
        return self.key.split('/')[0]

    @property
    def timeframe(self) -> str:
        return self.key.split('/')[1]

    @property
    def attribute_group(self) -> str:
        return self.key.split('/')[2]

    def df(self) -> pd.DataFrame:
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
            self.key, a.shape, a.dtype,
        )


class QueryResult(object):

    def __init__(self, result: Dict[str, np.ndarray], timezone: str):
        self.result = {
            key: DataSet(value, key, timezone)
            for key, value in six.iteritems(result)
        }
        self.timezone = timezone

    def keys(self) -> List[str]:
        return list(self.result.keys())

    def first(self) -> DataSet:
        return self.result[self.keys()[0]]

    def all(self) -> Dict[str, DataSet]:
        return self.result

    def __repr__(self):
        content = '\n'.join([
            str(ds) for _, ds in six.iteritems(self.result)
        ])
        return 'QueryResult({})'.format(content)


class QueryReply(object):

    def __init__(self, results, timezone):
        self.results = results
        self.timezone = timezone

    @classmethod
    def from_response(cls, resp: Dict):
        results = decode_responses(resp['responses'])
        return cls([QueryResult(result, resp['timezone']) for result in results], resp['timezone'])

    @classmethod
    def from_grpc_response(cls, resp: proto.MultiQueryResponse):  # ->QueryReply:
        results = decode_grpc_responses(resp.responses)
        return cls([QueryResult(result, resp.timezone) for result in results], resp.timezone)

    def first(self) -> DataSet:
        return self.results[0].first()

    def all(self) -> Dict[str, DataSet]:
        datasets = {}
        for result in self.results:
            datasets.update(result.all())
        return datasets

    def keys(self) -> List[str]:
        keys = []
        for result in self.results:
            keys += result.keys()
        return keys

    def get_catkeys(self, catnum: int) -> List[str]:
        ret = set()
        for key in self.keys():
            elems = key.split('/')
            ret.add(elems[catnum])
        return list(ret)

    def symbols(self) -> List[str]:
        return self.get_catkeys(0)

    def timeframes(self) -> List[str]:
        return self.get_catkeys(1)

    def by_symbols(self) -> Dict[str, DataSet]:
        datasets = self.all()
        ret = {}
        for key, dataset in six.iteritems(datasets):
            symbol = key.split('/')[0]
            ret[symbol] = dataset
        return ret

    def __repr__(self):
        content = '\n'.join([
            str(res) for res in self.results
        ])
        return 'QueryReply({})'.format(content)
