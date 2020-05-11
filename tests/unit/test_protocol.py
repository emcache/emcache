import asyncio
from unittest.mock import Mock

import pytest
from asynctest import CoroutineMock

from fastcache.protocol import MemcacheAsciiProtocol, create_protocol

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def protocol(event_loop):
    mock_transport = Mock()
    protocol = MemcacheAsciiProtocol()
    protocol._transport = mock_transport
    return protocol


class TestMemcacheAsciiProtocol:
    async def test_connection_made(self, event_loop):
        mock_transport = Mock()
        protocol = MemcacheAsciiProtocol()
        protocol.connection_made(mock_transport)
        assert protocol._transport is mock_transport

    async def test_connection_lost(self, event_loop):
        mock_transport = Mock()
        protocol = MemcacheAsciiProtocol()
        protocol.connection_made(mock_transport)
        protocol.connection_lost(None)

        # if it is closed does nothing
        protocol.close()
        protocol.connection_lost(None)

    async def test_close(self, event_loop):
        mock_transport = Mock()
        protocol = MemcacheAsciiProtocol()
        protocol.connection_made(mock_transport)

        # Calling many times will end up calling
        # only once to the transport close method
        protocol.close()
        protocol.close()
        protocol.close()

        protocol._transport.close.assert_called_once()

    async def test_get_cmd(self, event_loop, protocol):
        async def coro():
            await protocol.get_cmd(b"foo")

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VALUE foo 0 5\r\nvalue\r\nEND\r\n")

        await task

        protocol._transport.write.assert_called_with(b"get foo\r\n")

    async def test_set_cmd(self, event_loop, protocol):
        async def coro():
            await protocol.set_cmd(b"foo", b"value")

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"STORED\r\n")

        await task

        protocol._transport.write.assert_called_with(b"set foo 0 0 5\r\nvalue\r\n")


async def test_create_protocol(event_loop, mocker):
    loop_mock = Mock()
    mocker.patch("fastcache.protocol.asyncio.get_running_loop", return_value=loop_mock)

    protocol_mock = Mock()
    loop_mock.create_connection = CoroutineMock(return_value=(None, protocol_mock))

    protocol = await create_protocol("localhost", 11211)
    assert protocol is protocol_mock
    loop_mock.create_connection.assert_called_with(MemcacheAsciiProtocol, host="localhost", port=11211)
