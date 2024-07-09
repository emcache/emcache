# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from emcache import MemcachedHostAddress
from emcache.enums import Watcher
from emcache.protocol import ERROR, OK, MemcacheAsciiProtocol, create_protocol

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

        keys, values, flags, cas, client_error = await task

        assert keys == [b"foo"]
        assert values == [b"value"]
        assert flags == [0]
        assert cas == [None]
        assert client_error == bytearray()

        protocol._transport.write.assert_called_with(b"get foo\r\n")
        assert protocol._parser is None

    async def test_fetch_command_with_cas(self, event_loop, protocol):
        async def coro():
            return await protocol.fetch_command(b"gets", [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VALUE foo 0 5 1\r\nvalue\r\nEND\r\n")

        keys, values, flags, cas, client_error = await task

        assert keys == [b"foo"]
        assert values == [b"value"]
        assert flags == [0]
        assert cas == [1]
        assert client_error == bytearray()

        protocol._transport.write.assert_called_with(b"gets foo\r\n")

    async def test_fetch_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.fetch_command(b"get", [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
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

    async def test_incr_decr_command(self, event_loop, protocol):
        async def coro():
            return await protocol.incr_decr_command(b"incr", b"foo", 1, False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"2\r\n")

        result = await task

        assert result == b"2"

        protocol._transport.write.assert_called_with(b"incr foo 1\r\n")

    async def test_incr_decr_command_noreply(self, event_loop, protocol):
        await protocol.incr_decr_command(b"incr", b"foo", 1, True)
        protocol._transport.write.assert_called_with(b"incr foo 1 noreply\r\n")

    async def test_incr_decr_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.incr_decr_command(b"incr", b"foo", 1, False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_touch_command(self, event_loop, protocol):
        async def coro():
            return await protocol.touch_command(b"foo", 1, False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"TOUCHED\r\n")

        result = await task

        assert result == b"TOUCHED"

        protocol._transport.write.assert_called_with(b"touch foo 1\r\n")

    async def test_touch_command_noreply(self, event_loop, protocol):
        await protocol.touch_command(b"foo", 1, True)
        protocol._transport.write.assert_called_with(b"touch foo 1 noreply\r\n")

    async def test_touch_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.touch_command(b"foo", 1, False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_delete_command(self, event_loop, protocol):
        async def coro():
            return await protocol.delete_command(b"foo", False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"DELETED\r\n")

        result = await task

        assert result == b"DELETED"

        protocol._transport.write.assert_called_with(b"delete foo\r\n")

    async def test_delete_command_noreply(self, event_loop, protocol):
        await protocol.delete_command(b"foo", True)
        protocol._transport.write.assert_called_with(b"delete foo noreply\r\n")

    async def test_delete_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.delete_command(b"foo", False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_autodiscovery(self, event_loop, protocol):
        async def coro():
            return await protocol.autodiscovery()

        autodiscovery = True
        version = 12
        nodes = [
            ("myCluster.pc4ldq.0001.use1.cache.amazonaws.com", "10.82.235.120", 11211),
            ("myCluster.pc4ldq.0002.use1.cache.amazonaws.com", "10.80.249.27", 11211),
        ]

        response_config = b"%d\n" % version
        response_config += (
            " ".join("%s|%s|%d" % (hostname, ip, port) for hostname, ip, port in nodes)
        ).encode() + b"\n"
        response = b"CONFIG cluster 0 %d\r\n%s\r\nEND\r\n" % (len(response_config), response_config)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(response)

        result = await task

        assert result == (autodiscovery, version, nodes)

        protocol._transport.write.assert_called_with(b"config get cluster\r\n")

    async def test_autodiscovery_failure(self, event_loop, protocol):
        async def coro():
            return await protocol.autodiscovery()

        response = b"ERROR\r\n"

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(response)

        result = await task

        assert result == (False, -1, [])

        protocol._transport.write.assert_called_with(b"config get cluster\r\n")

    async def test_version_command(self, event_loop, protocol):
        async def coro():
            return await protocol.version_command()

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VERSION 1.6.26\r\n")

        result = await task

        assert result == b"VERSION 1.6.26"

        protocol._transport.write.assert_called_with(b"version\r\n")

    async def test_version_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.version_command()

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_get_and_touch_command(self, event_loop, protocol):
        async def coro():
            return await protocol.get_and_touch_command(b"gat", 0, [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VALUE foo 0 5\r\nvalue\r\nEND\r\n")

        keys, values, flags, cas, client_error = await task

        assert keys == [b"foo"]
        assert values == [b"value"]
        assert flags == [0]
        assert cas == [None]
        assert client_error == bytearray()

        protocol._transport.write.assert_called_with(b"gat 0 foo\r\n")
        assert protocol._parser is None

    async def test_get_and_touch_command_with_cas(self, event_loop, protocol):
        async def coro():
            return await protocol.get_and_touch_command(b"gats", 0, [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"VALUE foo 0 5 1\r\nvalue\r\nEND\r\n")

        keys, values, flags, cas, client_error = await task

        assert keys == [b"foo"]
        assert values == [b"value"]
        assert flags == [0]
        assert cas == [1]
        assert client_error == bytearray()

        protocol._transport.write.assert_called_with(b"gats 0 foo\r\n")

    async def test_get_and_touch_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.get_and_touch_command(b"gat", 0, [b"foo"])

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_cache_memlimit_command(self, event_loop, protocol):
        async def coro():
            return await protocol.cache_memlimit_command(64, noreply=False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"OK\r\n")

        result = await task

        assert result == OK

        protocol._transport.write.assert_called_with(b"cache_memlimit 64\r\n")

    async def test_cache_memlimit_command_noreply(self, event_loop, protocol):
        await protocol.cache_memlimit_command(64, noreply=True)
        protocol._transport.write.assert_called_with(b"cache_memlimit 64 noreply\r\n")

    async def test_cache_memlimit_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.cache_memlimit_command(1024, noreply=False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"ERROR\r\n")

        result = await task

        assert result == ERROR

        protocol._transport.write.assert_called_with(b"cache_memlimit 1024\r\n")

    async def test_stats_command(self, event_loop, protocol):
        async def coro():
            return await protocol.stats_command("sizes")

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"STAT sizes_status disabled\r\nEND\r\n")

        result = await task

        assert result == b"STAT sizes_status disabled\r\nEND"

        protocol._transport.write.assert_called_with(b"stats sizes\r\n")

    async def test_stats_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.stats_command()

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"ERROR\r\n")

        result = await task

        assert result == ERROR

        protocol._transport.write.assert_called_with(b"stats\r\n")

    async def test_verbosity_command(self, event_loop, protocol):
        async def coro():
            return await protocol.verbosity_command(1, False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"OK\r\n")

        result = await task

        assert result == b"OK"

        protocol._transport.write.assert_called_with(b"verbosity 1\r\n")

    async def test_verbosity_command_noreply(self, event_loop, protocol):
        await protocol.verbosity_command(1, True)
        protocol._transport.write.assert_called_with(b"verbosity 1 noreply\r\n")

    async def test_verbosity_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.verbosity_command(1, False)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert protocol._parser is None

    async def test_auth_command(self, event_loop, protocol):
        async def coro():
            return await protocol.auth_command("a", "a")

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"STORED\r\n")

        result = await task

        assert result == b"STORED"

        protocol._transport.write.assert_called_with(b"set _ _ _ 3\r\na a\r\n")
        assert protocol._parser is None

    async def test_auth_command_with_error(self, event_loop, protocol):
        async def coro():
            return await protocol.auth_command("a", "a")

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None

    async def test_watch_command(self, event_loop, protocol):
        async def my_event_handler():
            pass

        async def coro():
            return await protocol.watch_command(Watcher.fetchers, my_event_handler)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        protocol.data_received(b"OK\r\n")

        result = await task

        assert result == b"OK"

        protocol._transport.write.assert_called_with(b"watch fetchers\r\n")
        assert protocol._parser is None

    async def test_watch_command_with_error(self, event_loop, protocol):
        async def my_event_handler():
            pass

        async def coro():
            return await protocol.watch_command(Watcher.fetchers, my_event_handler)

        task = event_loop.create_task(coro())
        await asyncio.sleep(0)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # check that the protocol is yes or yes set to None
        assert protocol._parser is None


async def test_create_protocol(event_loop, mocker):
    loop_mock = Mock()
    mocker.patch("emcache.protocol.asyncio.get_running_loop", return_value=loop_mock)

    protocol_mock = Mock()
    protocol_mock.auth = AsyncMock()
    loop_mock.create_connection = AsyncMock(return_value=(None, protocol_mock))

    protocol = await create_protocol(
        MemcachedHostAddress("localhost", 11211),
        ssl=False,
        ssl_verify=False,
        ssl_extra_ca=None,
        username=None,
        password=None,
    )
    assert protocol is protocol_mock
    loop_mock.create_connection.assert_called_with(MemcacheAsciiProtocol, host="localhost", port=11211, ssl=False)
