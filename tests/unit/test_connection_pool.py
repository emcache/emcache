import asyncio
import time
from unittest.mock import Mock

import pytest
from asynctest import CoroutineMock

from fastcache.connection_pool import (
    BaseConnectionContext,
    ConnectionContext,
    ConnectionPool,
    WaitingForAConnectionContext,
)
from fastcache.default_values import (
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_PURGE_UNUSED_CONNECTIONS_AFTER,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def minimal_connection_pool():
    return ConnectionPool(
        "localhost",
        11211,
        max_connections=1,
        # Disable purge and connection timeout
        purge_unused_connections_after=None,
        connection_timeout=None,
    )


class TestConnectionPool:
    async def test_default_max_connections(self):
        connection_pool = ConnectionPool("localhost", 11211)
        assert connection_pool._max_connections == DEFAULT_MAX_CONNECTIONS

    async def test_default_purge_unused_connections_after(self):
        connection_pool = ConnectionPool("localhost", 11211)
        assert connection_pool._purge_unused_connections_after == DEFAULT_PURGE_UNUSED_CONNECTIONS_AFTER

    async def test_default_connection_timneout(self):
        connection_pool = ConnectionPool("localhost", 11211)
        assert connection_pool._connection_timeout == DEFAULT_CONNECTION_TIMEOUT

    async def test_str(self, minimal_connection_pool):
        assert str(minimal_connection_pool) == "<ConnectionPool host=localhost port=11211 total_connections=0>"
        assert repr(minimal_connection_pool) == "<ConnectionPool host=localhost port=11211 total_connections=0>"

    async def test_remove_waiter(self, minimal_connection_pool):
        waiter = Mock()
        minimal_connection_pool._waiters.append(waiter)
        minimal_connection_pool.remove_waiter(waiter)
        assert waiter not in minimal_connection_pool._waiters

    async def test_connection_context_connection(self, minimal_connection_pool):
        connection = Mock()

        # add a new connection into th pool
        minimal_connection_pool.release_connection(connection)

        # new connection should be used now
        connection_context = minimal_connection_pool.create_connection_context()
        async with connection_context as connection_from_pool:
            assert connection_from_pool is connection

    async def test_connection_context_waiting_connection(self, event_loop, mocker, minimal_connection_pool):
        connection = Mock()
        create_protocol = mocker.patch(
            "fastcache.connection_pool.create_protocol", CoroutineMock(return_value=connection)
        )

        async def coro(connection_context):
            async with connection_context as connection_from_pool:
                return connection_from_pool

        connection_context = minimal_connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        connection_from_pool = await task
        assert connection_from_pool is connection

        # check that we have called the create_protocol
        create_protocol.assert_called_with("localhost", 11211, timeout=None)

    async def test_connection_context_waiting_connection_with_timeout(self, event_loop, mocker):
        connection = Mock()
        create_protocol = mocker.patch(
            "fastcache.connection_pool.create_protocol", CoroutineMock(return_value=connection)
        )

        connection_pool = ConnectionPool("localhost", 11211, connection_timeout=1.0)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        await task

        # check that we have called the create_protocol
        create_protocol.assert_called_with("localhost", 11211, timeout=1.0)

    async def test_create_connection_with_timeout_error(self, event_loop, mocker):
        mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(side_effect=asyncio.TimeoutError))

        connection_pool = ConnectionPool("localhost", 11211, connection_timeout=1.0)
        await connection_pool._create_new_connection()
        assert connection_pool._total_connections == 0

    async def test_create_connection_with_oserror(self, event_loop, mocker):
        mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(side_effect=OSError))

        connection_pool = ConnectionPool("localhost", 11211, connection_timeout=1.0)
        await connection_pool._create_new_connection()
        assert connection_pool._total_connections == 0

    async def test_connection_context_one_create_connection(self, event_loop, mocker, minimal_connection_pool):
        # Checks that while there is an ongoing connection creation, mo more connections
        # will be created.
        create_protocol = mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(return_value=Mock()))

        async def coro(connection_context):
            async with connection_context as _:
                pass

        # Try to get a connection many times.
        connection_context1 = minimal_connection_pool.create_connection_context()
        connection_context2 = minimal_connection_pool.create_connection_context()
        connection_context3 = minimal_connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(*[task1, task2, task3])

        # check that we have called the create_protocol just once
        create_protocol.assert_called_once()

    async def test_connection_context_waiting_connection_max_connections(self, mocker, minimal_connection_pool):
        # Checks that once max connections have been reached, no more connections
        # will be created.
        create_protocol = mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock())

        # add a new connection into the pool and use it
        minimal_connection_pool.release_connection(Mock())
        _ = minimal_connection_pool.create_connection_context()

        # try to use a new one, since max_connections should be already
        # reached, the `create_protocol` should not be called
        _ = minimal_connection_pool.create_connection_context()

        create_protocol.assert_not_called()

    async def test_waiters_LIFO(self, event_loop, mocker, minimal_connection_pool):
        # Check that waiters queue are seen as LIFO queue, where we try to rescue the latency
        # of the last ones.
        mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(return_value=Mock()))

        waiters_woken_up = []

        async def coro(connection_context):
            async with connection_context as _:
                waiters_woken_up.append(connection_context)

        # Try to get a connection many times.
        connection_context1 = minimal_connection_pool.create_connection_context()
        connection_context2 = minimal_connection_pool.create_connection_context()
        connection_context3 = minimal_connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(*[task1, task2, task3])

        # check that where called in the right order
        assert waiters_woken_up == [connection_context3, connection_context2, connection_context1]

    async def test_waiters_cancellation_is_supported(self, event_loop, mocker, minimal_connection_pool):
        # Check that waiters that are cancelled are suported and do not break
        # the flow for wake up pending ones.

        mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(return_value=Mock()))

        waiters_woken_up = []

        begin = asyncio.Event()
        end = asyncio.Event()

        async def lazy_coro(connection_context):
            async with connection_context as _:
                begin.set()
                waiters_woken_up.append(connection_context)
                await end.wait()

        async def coro(connection_context):
            async with connection_context as _:
                waiters_woken_up.append(connection_context)

        connection_context1 = minimal_connection_pool.create_connection_context()
        connection_context2 = minimal_connection_pool.create_connection_context()
        connection_context3 = minimal_connection_pool.create_connection_context()

        # Because of the LIFO nature, the context 3 should be the first one
        # on having assigned the unique connection that will be available once
        #  is created.
        task1 = event_loop.create_task(lazy_coro(connection_context3))

        # Others will remain block until the first one finishes.
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context1))

        # waits till connection_context3 reaches the context
        await begin.wait()

        # cancel the connection_context2 that should be part
        #  of the waiters.
        task2.cancel()

        # notify the connection_context3 can move forward
        end.set()

        # wait till all tasks are finished
        await asyncio.gather(*[task1, task2, task3], return_exceptions=True)

        # check that where called in the right order, and the one cancelled
        # does not appear here.
        assert waiters_woken_up == [connection_context3, connection_context1]

        # check that there are no waiters pending, also the cancelled ones
        # was removed.
        assert len(minimal_connection_pool._waiters) == 0

    async def test_purge_connections(self, event_loop, mocker):

        # Mock everything, we will be calling it by hand
        get_running_loop = Mock()
        call_later = Mock()
        asyncio_patched = mocker.patch("fastcache.connection_pool.asyncio")
        asyncio_patched.get_running_loop = get_running_loop
        get_running_loop.return_value.call_later = call_later

        connection_pool = ConnectionPool("localhost", 11211, max_connections=2, purge_unused_connections_after=60)

        # Check that the call late was done with the right parameters
        call_later.assert_called_with(60, connection_pool._purge_unused_connections)

        # we add two connections by hand, with different timestamps.
        none_expired_connection = Mock()
        expired_connection = Mock()

        connection_pool._unused_connections.append(none_expired_connection)
        connection_pool._unused_connections.append(expired_connection)

        connection_pool._connections_last_time_used[none_expired_connection] = time.monotonic()
        connection_pool._connections_last_time_used[expired_connection] = time.monotonic() - 61

        # Run the purge
        connection_pool._purge_unused_connections()

        # Check that the expired has been removed and the none expired is still there
        assert expired_connection not in connection_pool._unused_connections
        assert none_expired_connection in connection_pool._unused_connections

        assert expired_connection not in connection_pool._connections_last_time_used
        assert none_expired_connection in connection_pool._connections_last_time_used

    async def test_purge_connections_disabled(self, event_loop, mocker):
        get_running_loop = Mock()
        call_later = Mock()
        asyncio_patched = mocker.patch("fastcache.connection_pool.asyncio")
        asyncio_patched.get_running_loop = get_running_loop
        get_running_loop.return_value.call_later = call_later

        ConnectionPool("localhost", 11211, max_connections=1, purge_unused_connections_after=None)

        # check that the mock wiring was done correctly by at least observing
        # the call to the `get_running_loop`
        assert get_running_loop.called

        # With a None the call_later should not be called
        assert not call_later.called

    async def test_create_and_update_connection_timestamp(self, event_loop, mocker):
        connection = Mock()
        mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(return_value=connection))

        now = time.monotonic()

        connection_pool = ConnectionPool("localhost", 11211, max_connections=1, purge_unused_connections_after=60)

        # Create a new connection and use it
        connection_context = connection_pool.create_connection_context()
        async with connection_context as _:
            # Check that the first timestamp saved is the one that was used
            # during the creation time.
            assert connection_pool._connections_last_time_used[connection] > now
            creation_timestamp = connection_pool._connections_last_time_used[connection]

        # Finally when the connection is relased back to the pool the timestamp
        # is also upated.
        assert connection_pool._connections_last_time_used[connection] > creation_timestamp


class TestBaseConnectionContext:
    async def test_at_enter_not_iplemented(self):
        with pytest.raises(NotImplementedError):
            async with BaseConnectionContext(Mock(), Mock(), None):
                pass

    async def test_release_connection_at_exit(self):
        class MyContext(BaseConnectionContext):
            async def __aenter__(self):
                pass

        connection_pool = Mock()
        connection = Mock()
        async with MyContext(connection_pool, connection, None):
            pass

        connection_pool.release_connection.assert_called_with(connection)


class TestConnectionContext:
    async def test_return_connection(self):
        connection_pool = Mock()
        connection = Mock()
        async with ConnectionContext(connection_pool, connection, None) as connection_returned:
            assert connection_returned is connection


class TestWaitingForAConnectionContext:
    async def tes_wait_for_a_connection(self, event_loop):

        connection = Mock()
        connection_pool = Mock()
        connection_pool.acquire_connection.return_value = connection
        waiter = event_loop.create_future()

        async def coro():
            async with WaitingForAConnectionContext(connection_pool, None, waiter) as connection_returned:
                assert connection_returned is connection

        task = event_loop.create_task(coro())
        waiter.set_result(None)
        await task
