from unittest.mock import Mock

import pytest
from asynctest import CoroutineMock

from emcache.protocol.ascii_ import MemcacheAsciiProtocol
from emcache.protocol.factory import create_protocol

pytestmark = pytest.mark.asyncio


async def test_create_protocol(event_loop, mocker):
    loop_mock = Mock()
    mocker.patch("emcache.protocol.factory.asyncio.get_running_loop", return_value=loop_mock)

    protocol_mock = Mock()
    loop_mock.create_connection = CoroutineMock(return_value=(None, protocol_mock))

    protocol = await create_protocol("localhost", 11211, ssl=False, ssl_verify=False, ssl_extra_ca=None)
    assert protocol is protocol_mock
    loop_mock.create_connection.assert_called_with(MemcacheAsciiProtocol, host="localhost", port=11211, ssl=False)
