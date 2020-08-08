import pymarketstore as pymkts


def test_init():
    p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
    tbk = "TSLA/1Min/OHLCV"
    assert p.tbk == tbk


def test_to_query_request():
    p = pymkts.Params('TSLA', '1Min', 'OHLCV', 1500000000, 4294967296)
    assert p.to_query_request() == {
        'destination': 'TSLA/1Min/OHLCV',
        'epoch_start': 1500000000,
        'epoch_end': 4294967296,
    }

    p2 = pymkts.Params(symbols=['FORD', 'TSLA'],
                       timeframe='5Min',
                       attrgroup='OHLCV',
                       start=1000000000,
                       end=4294967296,
                       limit=200,
                       limit_from_start=False)
    assert p2.to_query_request() == {
        'destination': 'FORD,TSLA/5Min/OHLCV',
        'epoch_start': 1000000000,
        'epoch_end': 4294967296,
        'limit_record_count': 200,
        'limit_from_start': False,
    }
