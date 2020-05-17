import asyncio
from unittest.mock import ANY, Mock

import pytest
from asynctest import CoroutineMock, MagicMock as AsyncMagicMock

from fastcache.client import MAX_ALLOWED_CAS_VALUE, MAX_ALLOWED_FLAG_VALUE, Client, OpTimeout
from fastcache.client_errors import StorageCommandError

pytestmark = pytest.mark.asyncio


class TestOpTimeout:
    async def test_timeout(self, event_loop):
        with pytest.raises(asyncio.TimeoutError):
            async with OpTimeout(0.01, event_loop):
                await asyncio.sleep(0.02)

    async def test_dont_timeout(self, event_loop):
        async with OpTimeout(0.02, event_loop):
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
    async def client(sel, event_loop, mocker):
        mocker.patch("fastcache.client.Cluster")
        return Client("localhost", 11211)

    async def test_max_allowed_cas_value(self, client):
        with pytest.raises(ValueError):
            await client.cas(b"foo", b"value", MAX_ALLOWED_CAS_VALUE + 1)

    async def test_max_allowed_flag_value(self, client):
        with pytest.raises(ValueError):
            await client.set(b"foo", b"value", flags=MAX_ALLOWED_FLAG_VALUE + 1)

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "replace"])
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
