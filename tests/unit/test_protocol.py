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

    async def test_fetch_command(self, event_loop, protocol):
        async def coro():
            return await protocol.fetch_command(b"get", [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VALUE foo 0 5\r\nvalue\r\nEND\r\n")

        keys, values, flags, cas = await task

        assert keys == [b"foo"]
        assert values == [b"value"]
        assert flags == [0]
        assert cas == [None]

        protocol._transport.write.assert_called_with(b"get foo\r\n")
        assert protocol._parser is None

    async def test_fetch_command_with_cas(self, event_loop, protocol):
        async def coro():
            return await protocol.fetch_command(b"gets", [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VALUE foo 0 5 1\r\nvalue\r\nEND\r\n")

        keys, values, flags, cas = await task

        assert keys == [b"foo"]
        assert values == [b"value"]
        assert flags == [0]
        assert cas == [1]

        protocol._transport.write.assert_called_with(b"gets foo\r\n")

    async def test_fetch_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.fetch_command(b"get", [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        #  check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_storage_command(self, event_loop, protocol):
        async def coro():
            return await protocol.storage_command(b"set", b"foo", b"value", 0, 0, False, cas=None)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"STORED\r\n")

        result = await task

        assert result == b"STORED"

        protocol._transport.write.assert_called_with(b"set foo 0 0 5\r\nvalue\r\n")

    async def test_storage_command_noreply(self, event_loop, protocol):
        await protocol.storage_command(b"set", b"foo", b"value", 0, 0, True, cas=None)
        protocol._transport.write.assert_called_with(b"set foo 0 0 5 noreply\r\nvalue\r\n")

    async def test_storage_command_with_cas(self, event_loop, protocol):
        await protocol.storage_command(b"cas", b"foo", b"value", 0, 0, True, cas=1)
        protocol._transport.write.assert_called_with(b"cas foo 0 0 5 1 noreply\r\nvalue\r\n")

    async def test_storage_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.storage_command(b"set", b"foo", b"value", 0, 0, False, cas=None)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert protocol._parser is None


async def test_create_protocol(event_loop, mocker):
    loop_mock = Mock()
    mocker.patch("fastcache.protocol.asyncio.get_running_loop", return_value=loop_mock)

    protocol_mock = Mock()
    loop_mock.create_connection = CoroutineMock(return_value=(None, protocol_mock))

    protocol = await create_protocol("localhost", 11211)
    assert protocol is protocol_mock
    loop_mock.create_connection.assert_called_with(MemcacheAsciiProtocol, host="localhost", port=11211)
