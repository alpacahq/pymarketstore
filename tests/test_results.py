import pandas as pd

from ast import literal_eval
from pymarketstore import results
import imp
imp.reload(results)


def assert_dataframes_equal(got, expected):
    for i in expected.index:
        row_bool = expected.loc[i] == got.loc[i]
        if not row_bool.all():
            print('got:\n', got.loc[i])
            print('expected:\n', expected.loc[i])
            raise AssertionError


testdata1 = literal_eval(r"""
{'responses': [{'result': {'data': [b'\xf4\xe8^Z\x00\x00\x00\x000\xe9^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00',
     b'{\x14\xaeG\x01\xf4\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@',
     b'{\x14\xaeG\x01\xf4\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\xfe\xc5@',
     b'\x85\xebQ\xb8^\xe0\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@{\x14\xaeG\x01\xfa\xc5@',
     b'H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\xfe\xfd\xc5@',
     b'iL\xd2F\xbf\xaf\n@\xfe\xe6\xff49\xfd\x0b@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xd7\xd2\x8a\x0c\xfe\x00\xf9?'],
    'length': 5,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'UTC',
 'version': 'dev'}
""")  # noqa: E501

testdata2 = literal_eval(r"""
{'responses': [{'result': {'data': [b'l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00 \xea^Z\x00\x00\x00\x00\\\xea^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00 \xea^Z\x00\x00\x00\x00\\\xea^Z\x00\x00\x00\x00',
     b'\x00\x00\x00\x00\x00\x88\x8f@)\\\x8f\xc2\xf5\x90\x8f@\xa4p=\n\xd7\x8f\x8f@\xcd\xcc\xcc\xcc\xcc\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x00\x00\x00\x00 \x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\x00\x00\x00\x00\x00\xb0\x8f@fffff\xa2\x8f@\x00\x00\x00\x00\x00\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\x00\x00\x00\x00\x00\x88\x8f@{\x14\xaeG\xe1\x84\x8f@\xa4p=\n\xd7\x8f\x8f@\xf6(\\\x8f\xc2\xa7\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@\\\x8f\xc2\xf5h\xf0\xc5@\x00\x00\x00\x00 \x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\xd7\xa3p=\n\x99\x8f@\xa4p=\n\xd7\x8f\x8f@\x00\x00\x00\x00\x00\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\x1e\x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'f\r\x83\x9erg8@j\xa8\xcd\x0f\x8e\xdf<@\x7f\xc7\xa6Ku\xbcP@AG\xe5\x05\xdc\x1aU@\xdc\xb1d\xd0\x012+@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xa2\x9a\xa3\xd8\x1bb\x19@s!\xc1\x1a\x88\xa9/@\xbaI\x0c\x02+\x87\xf4?'],
    'length': 10,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5,
     'ETH/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5,
     'ETH/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'America/New_York',
 'version': 'dev'}
""")  # noqa: E501


def test_results():
    reply = results.QueryReply(testdata1)
    assert reply.timezone == 'UTC'
    assert str(reply) == """QueryReply(QueryResult(DataSet(key=BTC/1Min/OHLCV, shape=(5,), dtype=[('Epoch', '<i8'), ('Open', '<f8'), ('High', '<f8'), ('Low', '<f8'), ('Close', '<f8'), ('Volume', '<f8')])))"""  # noqa
    assert reply.first().timezone == 'UTC'
    assert reply.first().symbol == 'BTC'
    assert reply.first().timeframe == '1Min'
    assert reply.first().attribute_group == 'OHLCV'
    assert reply.first().df().shape == (5, 5)
    assert list(reply.by_symbols().keys()) == ['BTC']
    assert reply.keys() == ['BTC/1Min/OHLCV']
    assert reply.symbols() == ['BTC']
    assert reply.timeframes() == ['1Min']

    reply = results.QueryReply(testdata2)
    assert str(reply.first().df().index.tzinfo) == 'America/New_York'

    expected = pd.DataFrame([
        (pd.Timestamp('2018-01-17 06:17:00+00:00'), 11276.0, 11276.0, 11276.0, 11276.0, 1.283),
        (pd.Timestamp('2018-01-17 06:17:00+00:00'), 1014.0, 1014.0, 1014.0, 1014.0,
         13.597670090000001),
    ],
        columns=['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
        index=['BTC', 'ETH'],
    )
    assert_dataframes_equal(reply.latest_df(), expected)
