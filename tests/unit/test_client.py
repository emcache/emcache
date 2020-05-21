import asyncio
from unittest.mock import ANY, Mock

import pytest
from asynctest import CoroutineMock, MagicMock as AsyncMagicMock

from emcache.client import MAX_ALLOWED_CAS_VALUE, MAX_ALLOWED_FLAG_VALUE, Client, OpTimeout, create_client
from emcache.client_errors import StorageCommandError
from emcache.default_values import (
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_PURGE_UNUSED_CONNECTIONS_AFTER,
    DEFAULT_TIMEOUT,
)

pytestmark = pytest.mark.asyncio


class TestOpTimeout:
    async def test_timeout(self, event_loop):
        with pytest.raises(asyncio.TimeoutError):
            async with OpTimeout(0.01, event_loop):
                await asyncio.sleep(1)

    async def test_dont_timeout(self, event_loop):
        async with OpTimeout(1, event_loop):
            await asyncio.sleep(0.01)

    async def test_cancellation_is_supported(self, event_loop):
        ev = asyncio.Event()

        async def coro():
            async with OpTimeout(0.01, event_loop):
                ev.set()
                await asyncio.sleep(0.02)

        task = event_loop.create_task(coro())
        await ev.wait()

        # We cancel before the timeout is triggered
        task.cancel()

        # we should observe a cancellation rather than
        #  a timeout error.
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_check_cancel_timer_handler(self):
        # When a timeout is not triggered, time handler
        # must be cancelled

        timer_handler = Mock()
        loop = Mock()
        loop.call_later.return_value = timer_handler
        async with OpTimeout(0.01, loop):
            pass

        loop.call_later.assert_called_with(0.01, ANY)
        timer_handler.cancel.assert_called()

    async def test_check_cancel_timer_handler_when_exception_triggers(self):
        # the same but having an exception
        timer_handler = Mock()
        loop = Mock()
        loop.call_later.return_value = timer_handler

        with pytest.raises(Exception):
            async with OpTimeout(0.01, loop):
                raise Exception

        timer_handler.cancel.assert_called()


class TestClient:
    """ Only none happy path tests, happy path tests are
    covered as acceptance test.
    """

    @pytest.fixture
    async def client(self, event_loop, mocker):
        mocker.patch("emcache.client.Cluster")
        return Client([("localhost", 11211)], None, 1, None, None)

    async def test_invalid_host_addresses(self):
        with pytest.raises(ValueError):
            Client([], None, 1, None, None)

    async def test_max_allowed_cas_value(self, client):
        with pytest.raises(ValueError):
            await client.cas(b"foo", b"value", MAX_ALLOWED_CAS_VALUE + 1)

    async def test_max_allowed_flag_value(self, client):
        with pytest.raises(ValueError):
            await client.set(b"foo", b"value", flags=MAX_ALLOWED_FLAG_VALUE + 1)

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "prepend", "replace"])
    async def test_not_stored_error_storage_command(self, client, command):
        # patch what is necesary for returnning an error string
        connection = CoroutineMock()
        connection.storage_command = CoroutineMock(return_value=b"ERROR")
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(StorageCommandError):
            f = getattr(client, command)
            await f(b"foo", b"value")

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "prepend", "replace"])
    async def test_invalid_key(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(b"\n", b"value")

    async def test_cas_not_stored_error_storage_command(self, client):
        # patch what is necesary for returnning an error string
        connection = CoroutineMock()
        connection.storage_command = CoroutineMock(return_value=b"ERROR")
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(StorageCommandError):
            await client.cas(b"foo", b"value", 1)

    async def test_cas_invalid_key(self, client):
        with pytest.raises(ValueError):
            await client.cas(b"\n", b"value", 1)

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_empty_keys(self, client, command):
        f = getattr(client, command)
        result = await f([])
        assert result == {}

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_invalid_keys(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f([b"\n"])

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_exception_cancels_everything(self, client, command):
        # patch what is necesary for rasing an exception for the first query and
        # a "valid" response from the others
        connection = CoroutineMock()
        connection.fetch_command.side_effect = CoroutineMock(side_effect=[OSError(), b"Ok", b"Ok"])
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node1 = Mock()
        node2 = Mock()
        node3 = Mock()
        node1.connection.return_value = connection_context
        node2.connection.return_value = connection_context
        node3.connection.return_value = connection_context
        client._cluster.pick_nodes.return_value = {node1: [b"key1"], node2: [b"key2"], node3: [b"key3"]}
        with pytest.raises(OSError):
            f = getattr(client, command)
            await f([b"key1", b"key2", b"key3"])


async def test_create_client_default_values(event_loop, mocker):
    client_class = mocker.patch("emcache.client.Client")
    await create_client([("localhost", 11211)])
    client_class.assert_called_with(
        [("localhost", 11211)],
        DEFAULT_TIMEOUT,
        DEFAULT_MAX_CONNECTIONS,
        DEFAULT_PURGE_UNUSED_CONNECTIONS_AFTER,
        DEFAULT_CONNECTION_TIMEOUT,
    )
