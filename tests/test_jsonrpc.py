from pymarketstore import jsonrpc
from pymarketstore.exceptions import SymbolsError

import pytest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import imp
imp.reload(jsonrpc)


@patch.object(jsonrpc, 'requests')
def test_jsonrpc(requests):
    requests.Session().post.return_value = 'dummy_data'

    cli = jsonrpc.MsgpackRpcClient('http://localhost:5993/rcp')
    result = cli.call('DataService.Query', a=1)
    assert result == 'dummy_data'
    resp = {
        'jsonrpc': '2.0',
        'id': 1,
        'result': {'ok': True},
    }
    assert cli.response(resp)['ok']

    del resp['result']
    resp['error'] = {
        'message': 'Error',
        'data': 'something',
    }
    with pytest.raises(Exception):
        cli.response(resp)

    resp['error'] = {
        'message': 'No files returned from query parse',
        'data': 'something',
    }
    with pytest.raises(SymbolsError):
        cli.response(resp)
