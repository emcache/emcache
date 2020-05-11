import asyncio
from unittest.mock import Mock

import pytest
from asynctest import CoroutineMock

from fastcache.connection_pool import (
    BaseConnectionContext,
    ConnectionContext,
    ConnectionPool,
    WaitingForAConnectionContext,
)

pytestmark = pytest.mark.asyncio


class TestConnectionPool:
    async def test_str(self):
        connection_pool = ConnectionPool("localhost", 11211, 1)
        assert str(connection_pool) == "<ConnectionPool host=localhost port=11211 total_connections=0>"
        assert repr(connection_pool) == "<ConnectionPool host=localhost port=11211 total_connections=0>"

    async def test_remove_waiter(self):
        waiter = Mock()
        connection_pool = ConnectionPool("localhost", 11211, 1)
        connection_pool._waiters.append(waiter)
        connection_pool.remove_waiter(waiter)
        assert waiter not in connection_pool._waiters

    async def test_connection_context_connection(self):
        connection = Mock()
        connection_pool = ConnectionPool("localhost", 11211, 1)

        # add a new connection into th pool
        connection_pool.release_connection(connection)

        # new connection should be used now
        connection_context = connection_pool.create_connection_context()
        async with connection_context as connection_from_pool:
            assert connection_from_pool is connection

    async def test_connection_context_waiting_connection(self, event_loop, mocker):
        connection = Mock()
        create_protocol = mocker.patch(
            "fastcache.connection_pool.create_protocol", CoroutineMock(return_value=connection)
        )

        async def coro(connection_context):
            async with connection_context as connection_from_pool:
                return connection_from_pool

        connection_pool = ConnectionPool("localhost", 11211, 1)
        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        connection_from_pool = await task
        assert connection_from_pool is connection

        # check that we have called the create_protocol
        create_protocol.assert_called_with("localhost", 11211)

    async def test_connection_context_one_create_connection(self, event_loop, mocker):
        # Checks that while there is an ongoing connection creation, mo more connections
        # will be created.
        create_protocol = mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(return_value=Mock()))

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_pool = ConnectionPool("localhost", 11211, 1)

        # Try to get a connection many times.
        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(*[task1, task2, task3])

        # check that we have called the create_protocol just once
        create_protocol.assert_called_once()

    async def test_connection_context_waiting_connection_max_connections(self, mocker):
        # Checks that once max connections have been reached, no more connections
        # will be created.
        create_protocol = mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock())

        connection_pool = ConnectionPool("localhost", 11211, 1)

        # add a new connection into the pool and use it
        connection_pool.release_connection(Mock())
        _ = connection_pool.create_connection_context()

        # try to use a new one, since max_connections should be already
        # reached, the `create_protocol` should not be called
        _ = connection_pool.create_connection_context()

        create_protocol.assert_not_called()

    async def test_waiters_LIFO(self, event_loop, mocker):
        # Check that waiters queue are seen as LIFO queue, where we try to rescue the latency
        # of the last ones.
        mocker.patch("fastcache.connection_pool.create_protocol", CoroutineMock(return_value=Mock()))

        waiters_woken_up = []

        async def coro(connection_context):
            async with connection_context as _:
                waiters_woken_up.append(connection_context)

        connection_pool = ConnectionPool("localhost", 11211, 1)

        # Try to get a connection many times.
        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(*[task1, task2, task3])

        # check that where called in the right order
        assert waiters_woken_up == [connection_context3, connection_context2, connection_context1]

    async def test_waiters_cancellation_is_supported(self, event_loop, mocker):
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

        connection_pool = ConnectionPool("localhost", 11211, 1)

        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

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
        assert len(connection_pool._waiters) == 0


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
