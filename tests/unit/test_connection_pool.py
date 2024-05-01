# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import time
from unittest.mock import AsyncMock, Mock, call

import pytest

from emcache.connection_pool import (
    _MAX_CREATE_CONNECTION_LATENCIES_OBSERVED,
    CREATE_CONNECTION_MAX_BACKOFF,
    DECLARE_UNHEALTHY_CONNECTION_POOL_AFTER_RETRIES,
    BaseConnectionContext,
    ConnectionContext,
    ConnectionPool,
    ConnectionPoolMetrics,
    WaitingForAConnectionContext,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def minimal_connection_pool(mocker):
    mocker.patch("emcache.connection_pool.create_protocol", AsyncMock())
    return ConnectionPool(
        ("localhost", 11211),
        1,
        1,
        # Disable purge and connection timeout
        None,
        None,
        lambda _: _,
        False,
        False,
        None,
    )


class TestConnectionPool:
    @pytest.fixture
    def not_closed_connection(self):
        connection = Mock()
        connection.closed.return_value = False
        return connection

    async def test_str(self, minimal_connection_pool):
        assert str(minimal_connection_pool) == (
            "<ConnectionPool host=localhost port=11211 total_connections=1 "
            + "min_connections=1 max_connections=1 closed=False>"
        )
        assert (
            repr(minimal_connection_pool) == "<ConnectionPool host=localhost port=11211 total_connections=1 "
            "min_connections=1 max_connections=1 closed=False>"
        )

    async def test_minimum_max_connections(self):
        with pytest.raises(ValueError):
            ConnectionPool(("localhost", 11211), 0, 1, None, None, lambda _: _, False, False, None)

    async def test_minimum_min_connections(self):
        with pytest.raises(ValueError):
            ConnectionPool(("localhost", 11211), 1, -1, None, None, lambda _: _, False, False, None)

    async def test_maximum_min_connections(self):
        with pytest.raises(ValueError):
            ConnectionPool(("localhost", 11211), 1, 2, None, None, lambda _: _, False, False, None)

    async def test_close(self, mocker):
        connection = Mock()
        ev = asyncio.Event()

        def f(*args, **kwargs):
            ev.set()
            return connection

        _ = mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(side_effect=f))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        # wait till the create_connection has ben called and a connection has been
        # returned.
        await ev.wait()

        await connection_pool.close()

        # test that the connection has been closed
        connection.close.assert_called()

    async def test_metrics(self):
        # Just test that the type returned is the right one and there is a method
        # published. Specific attribute value are tested in the other tests which are
        # not specific to metrics but they are simulating most of the situations that
        # are needed for incrementing the counters.
        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)
        assert isinstance(connection_pool.metrics(), ConnectionPoolMetrics)

        # checks that without latencies create connection times are None values
        assert connection_pool.metrics().create_connection_avg is None
        assert connection_pool.metrics().create_connection_p50 is None
        assert connection_pool.metrics().create_connection_p99 is None
        assert connection_pool.metrics().create_connection_upper is None

        await connection_pool.close()

    async def test_metrics_create_connection_latencies(self):
        # Check that the latencies measured are well calculated when metrics
        # method is called.
        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)
        connection_pool._create_connection_latencies = [float(i) for i in range(0, 101)]

        assert connection_pool.metrics().create_connection_avg == 50.0
        assert connection_pool.metrics().create_connection_p50 == 50.0
        assert connection_pool.metrics().create_connection_p99 == 99.0
        assert connection_pool.metrics().create_connection_upper == 100.0

        await connection_pool.close()

    async def test_remove_waiter(self, minimal_connection_pool):
        waiter = Mock()
        minimal_connection_pool._waiters.append(waiter)
        minimal_connection_pool.remove_waiter(waiter)
        assert waiter not in minimal_connection_pool._waiters

    @pytest.mark.parametrize("min_connections", [1, 10])
    async def test_initalizes_creating_min_connection(self, mocker, not_closed_connection, min_connections):
        ev = asyncio.Event()
        connections_created = 0

        def f(*args, **kwargs):
            nonlocal connections_created
            connections_created += 1
            if connections_created == min_connections:
                ev.set()
            return not_closed_connection

        create_protocol = mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(side_effect=f))

        max_connections = min_connections
        connection_pool = ConnectionPool(
            ("localhost", 11211), max_connections, min_connections, None, None, lambda _: _, False, False, None
        )

        # wait till the min_connections have been created.
        await ev.wait()

        create_protocol.assert_called_with(
            ("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=None
        )
        assert connection_pool.total_connections == min_connections

        # test specific atributes of the metrics
        assert connection_pool.metrics().cur_connections == min_connections
        assert connection_pool.metrics().connections_created == min_connections
        assert connection_pool.metrics().create_connection_avg is not None
        assert connection_pool.metrics().create_connection_p50 is not None
        assert connection_pool.metrics().create_connection_p99 is not None
        assert connection_pool.metrics().create_connection_upper is not None

    async def test_create_connection_with_timeout(self, mocker, not_closed_connection):
        ev = asyncio.Event()

        def f(*args, **kwargs):
            ev.set()
            return not_closed_connection

        create_protocol = mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(side_effect=f))

        _ = ConnectionPool(("localhost", 11211), 1, 1, None, 1.0, lambda _: _, False, False, None)

        # wait till the create_connection has ben called and a connection has been
        # returned.
        await ev.wait()

        create_protocol.assert_called_with(
            ("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=1.0
        )

    async def test_create_connection_max_observed_latencies(self, event_loop, mocker, not_closed_connection):
        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        # Perform twice operations than the maximum observed latencies by removing
        # the connection at each operation
        for i in range(_MAX_CREATE_CONNECTION_LATENCIES_OBSERVED * 2):
            async with connection_pool.create_connection_context():
                pass

            # remove the connection explicitly, behind the scenes connection pool
            # will be forced to create a new one
            connection_pool._close_connection(not_closed_connection)

        # We should never have more than the maximum number of latencies observed
        assert len(connection_pool._create_connection_latencies) == _MAX_CREATE_CONNECTION_LATENCIES_OBSERVED

    async def test_connection_context_connection(self, mocker, not_closed_connection):
        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        connection_context = connection_pool.create_connection_context()
        async with connection_context as connection_from_pool:
            assert connection_from_pool is not_closed_connection

    @pytest.mark.parametrize("exc", [asyncio.TimeoutError, OSError])
    async def test_create_connection_tries_again_if_error(self, event_loop, mocker, not_closed_connection, exc):
        # If there is an error trying to acquire a connection and there are waiters waiting we will keep
        # trying to create a connection

        # Avoid the annoying sleep added by the backoff
        mocker.patch("emcache.connection_pool.asyncio.sleep", AsyncMock())

        # First we return a Timeout and second try returns an usable connection
        create_protocol = mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=[exc, not_closed_connection]),
        )

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, 1.0, lambda _: _, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        await task

        # check that we have called twice the create_protocol
        create_protocol.assert_has_calls(
            [
                call(("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=1.0),
                call(("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=1.0),
            ]
        )

        # test specific atributes of the metrics
        assert connection_pool.metrics().cur_connections == 1
        assert connection_pool.metrics().connections_created == 1
        assert connection_pool.metrics().connections_created_with_error == 1

    async def test_create_connection_do_not_try_again_unknown_error(self, event_loop, mocker):
        # If there is an error trying to acquire a connection and there are waiters waiting we will keep
        # trying to create a connection

        # Avoid the annoying sleep added by the backoff
        mocker.patch("emcache.connection_pool.asyncio.sleep", AsyncMock())

        create_protocol = mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=Exception),
        )

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, 1.0, lambda _: _, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=0.01)

        # check that we have called once the create_protocol
        create_protocol.assert_called_once_with(
            ("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=1.0
        )

        assert connection_pool.total_connections == 0

    async def test_create_connection_backoff(self, event_loop, mocker, not_closed_connection):
        sleep = mocker.patch("emcache.connection_pool.asyncio.sleep", AsyncMock())

        # Simulate as many timeouts as many errors would need to happen for
        # reaching the max backoff plus 1
        errors = [asyncio.TimeoutError] * 8

        mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=errors + [not_closed_connection]),
        )

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, 1.0, lambda _: _, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        await task

        # check that the sleep call was called with the right values
        sleep.assert_has_calls(
            [
                call(1),
                call(2),
                call(4),
                call(8),
                call(16),
                call(32),
                call(CREATE_CONNECTION_MAX_BACKOFF),
                call(CREATE_CONNECTION_MAX_BACKOFF),
            ]
        )

    async def test_connection_context_one_create_connection(self, event_loop, mocker, not_closed_connection):
        # Checks that while there is an ongoing connection creation, mo more connections
        # will be created.
        create_protocol = mocker.patch(
            "emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection)
        )

        connection_pool = ConnectionPool(("localhost", 11211), 3, 1, None, None, lambda _: _, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        # Try to get a connection many times.
        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(task1, task2, task3)

        # check that we have called the create_protocol just once
        create_protocol.assert_called_once()

    async def test_connection_context_max_connections(self, event_loop, mocker, not_closed_connection):
        # Checks that once max connections have been reached, no more connections
        # will be created.
        create_protocol = mocker.patch(
            "emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection)
        )

        # Limit the number of maximum connection to 2
        connection_pool = ConnectionPool(("localhost", 11211), 2, 1, None, None, lambda _: _, False, False, None)

        # Events used for having only at most one concurrent create_connection, otherwise
        # we would suffer the dogpile side effect and only one connection will be created
        # becuse connection pool does not allow to create more than one connection every time.
        ev_1 = asyncio.Event()
        ev_2 = asyncio.Event()
        ev_3 = asyncio.Event()
        ev_4 = asyncio.Event()

        async def command_1():
            async with connection_pool.create_connection_context() as _:
                ev_1.set()
                await ev_4.wait()

        async def command_2():
            await ev_1.wait()
            async with connection_pool.create_connection_context() as _:
                ev_2.set()
                await ev_4.wait()

        async def command_3():
            await ev_1.wait()
            await ev_2.wait()
            ev_3.set()
            async with connection_pool.create_connection_context() as _:
                await ev_4.wait()

        async def command_4():
            await ev_1.wait()
            await ev_2.wait()
            await ev_3.wait()
            ev_4.set()
            async with connection_pool.create_connection_context() as _:
                pass

        task1 = event_loop.create_task(command_1())
        task2 = event_loop.create_task(command_2())
        task3 = event_loop.create_task(command_3())
        task4 = event_loop.create_task(command_4())

        await asyncio.gather(task1, task2, task3, task4)

        # check that we have called the create_protocol only twice
        assert create_protocol.call_count == 2
        assert connection_pool.total_connections == 2

    async def test_connection_context_not_use_a_closed_connections(self, event_loop, mocker, not_closed_connection):
        # Checks that if a connection is in closed state its not used and purged.
        connection_closed = Mock()
        connection_closed.closed.return_value = True

        connection_pool = ConnectionPool(("localhost", 11211), 2, 1, None, None, lambda _: _, False, False, None)

        # we add by hand a closed connection and a none closed one to the pool
        connection_pool._unused_connections.append(not_closed_connection)
        connection_pool._unused_connections.append(connection_closed)
        connection_pool._total_connections = 2

        connection_context = connection_pool.create_connection_context()

        async with connection_context as connection:
            assert connection is not_closed_connection

        # double check that the closed one has been removed and the none closed
        # one is still there.
        assert not_closed_connection in connection_pool._unused_connections
        assert connection_closed not in connection_pool._unused_connections
        assert connection_pool.total_connections == 1

        # test specific atributes of the metrics
        assert connection_pool.metrics().connections_closed == 1

    async def test_connection_context_create_connection_if_closed(self, event_loop, mocker, not_closed_connection):
        # Checks that if a connection is returned in closed state, if there are waiters a new connection is created.

        connection_closed = Mock()
        connection_closed.closed.return_value = True
        create_protocol = mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=[connection_closed, not_closed_connection]),
        )

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        # Try to get a connection many times. The first one will
        # get and use the connection_closed, when this connection is
        # returned back to the pool, a new one will be created
        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(task1, task2, task3)

        # check that we have called the create_protocol twice
        create_protocol.assert_has_calls(
            [
                call(("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=None),
                call(("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=None),
            ]
        )
        assert connection_pool.total_connections == 1

        # test specific atributes of the metrics
        assert connection_pool.metrics().connections_closed == 1

    async def test_connection_context_create_connection_if_context_returns_with_exception(
        self, event_loop, mocker, not_closed_connection
    ):
        # Checks that if a connection returns with an exception, if there are waiters a new connection is created.
        create_protocol = mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=[not_closed_connection, not_closed_connection]),
        )

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        async def coro(connection_context, raise_exception=False):
            async with connection_context as _:
                if raise_exception:
                    raise Exception()

        # Try to get a connection many times. The first one will
        # get and use the connection_closed, when this connection is
        # returned back to the pool, a new one will be created
        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))

        # Remember that due to the LIFO nature, we have to raise the exception using the last
        # pushed connection context
        task3 = event_loop.create_task(coro(connection_context3, raise_exception=True))

        await asyncio.gather(task1, task2, task3, return_exceptions=True)

        # check that we have called the create_protocol twice
        create_protocol.assert_has_calls(
            [
                call(("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=None),
                call(("localhost", 11211), ssl=False, ssl_verify=False, ssl_extra_ca=None, timeout=None),
            ]
        )
        assert connection_pool.total_connections == 1

        # test specific atributes of the metrics
        assert connection_pool.metrics().connections_closed == 1
        assert connection_pool.metrics().operations_executed_with_error == 1
        assert connection_pool.metrics().operations_executed == 2

    async def test_waiters_LIFO(self, event_loop, mocker, not_closed_connection):
        # Check that waiters queue are seen as LIFO queue, where we try to rescue the latency
        # of the last ones.
        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        waiters_woken_up = []

        async def coro(connection_context):
            async with connection_context as _:
                waiters_woken_up.append(connection_context)

        # Try to get a connection many times.
        connection_context1 = connection_pool.create_connection_context()
        connection_context2 = connection_pool.create_connection_context()
        connection_context3 = connection_pool.create_connection_context()

        task1 = event_loop.create_task(coro(connection_context1))
        task2 = event_loop.create_task(coro(connection_context2))
        task3 = event_loop.create_task(coro(connection_context3))

        await asyncio.gather(task1, task2, task3)

        # check that where called in the right order
        assert waiters_woken_up == [connection_context3, connection_context2, connection_context1]

        # test specific atributes of the metrics
        assert connection_pool.metrics().operations_waited == 3

    async def test_connections_are_released_if_waiter_is_cancelled_just_after_wakeup(
        self, event_loop, mocker, not_closed_connection
    ):
        async def never_return_connection(*args, **kwargs):
            await asyncio.sleep(1e10)

        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(side_effect=never_return_connection))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context1 = connection_pool.create_connection_context()
        task1 = event_loop.create_task(coro(connection_context1))

        # wait till all tasks are waiting
        await asyncio.sleep(0.1)

        with pytest.raises(asyncio.CancelledError):
            # Force waiter to be waken up
            connection_pool._wakeup_next_waiter_or_append_to_unused(not_closed_connection)
            # Cancel task immediately after
            task1.cancel()
            await task1

        # Waiter should have been removed too
        assert len(connection_pool._waiters) == 0

    async def test_waiters_cancellation_is_supported(self, event_loop, mocker, not_closed_connection):
        # Check that waiters that are cancelled are suported and do not break
        # the flow for wake up pending ones.

        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

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
        await asyncio.gather(task1, task2, task3, return_exceptions=True)

        # check that where called in the right order, and the one cancelled
        # does not appear here.
        assert waiters_woken_up == [connection_context3, connection_context1]

        # check that there are no waiters pending, also the cancelled ones
        # was removed.
        assert len(connection_pool._waiters) == 0

    async def test_purge_connections(self, event_loop, mocker):

        # Mock everything, we will be calling it by hand
        get_running_loop = Mock()
        call_later = Mock()
        asyncio_patched = mocker.patch("emcache.connection_pool.asyncio")
        asyncio_patched.get_running_loop = get_running_loop
        get_running_loop.return_value.call_later = call_later

        connection_pool = ConnectionPool(("localhost", 11211), 3, 1, 60, None, lambda _: _, False, False, None)

        # Check that the call late was done with the right parameters
        call_later.assert_called_with(60, connection_pool._purge_unused_connections)

        # we add three connections by hand, with different timestamps.
        none_expired_connection = Mock()
        expired_connection_1 = Mock()
        expired_connection_2 = Mock()

        connection_pool._unused_connections.append(none_expired_connection)
        connection_pool._unused_connections.append(expired_connection_1)
        connection_pool._unused_connections.append(expired_connection_2)

        connection_pool._connections_last_time_used[none_expired_connection] = time.monotonic()
        connection_pool._connections_last_time_used[expired_connection_1] = time.monotonic() - 61
        connection_pool._connections_last_time_used[expired_connection_2] = time.monotonic() - 61

        connection_pool._total_connections = 3

        # Run the purge
        connection_pool._purge_unused_connections()

        # Check that only one expired connection has been removed and the none expired is still there
        assert expired_connection_1 not in connection_pool._unused_connections
        assert expired_connection_2 in connection_pool._unused_connections
        assert none_expired_connection in connection_pool._unused_connections

        assert expired_connection_1 not in connection_pool._connections_last_time_used
        assert expired_connection_2 in connection_pool._connections_last_time_used
        assert none_expired_connection in connection_pool._connections_last_time_used

        assert connection_pool.total_connections == 2

        assert connection_pool.metrics().cur_connections == 2
        assert connection_pool.metrics().connections_purged == 1

        # Run the purge again, which should purge the other unused connection
        connection_pool._purge_unused_connections()

        assert expired_connection_2 not in connection_pool._unused_connections
        assert none_expired_connection in connection_pool._unused_connections

        assert expired_connection_2 not in connection_pool._connections_last_time_used
        assert none_expired_connection in connection_pool._connections_last_time_used

        assert connection_pool.total_connections == 1

        assert connection_pool.metrics().cur_connections == 1
        assert connection_pool.metrics().connections_purged == 2

        # Run the purge again, nothing should happen
        connection_pool._purge_unused_connections()

        assert none_expired_connection in connection_pool._unused_connections
        assert none_expired_connection in connection_pool._connections_last_time_used
        assert connection_pool.total_connections == 1

    @pytest.mark.parametrize("min_connections", [1, 2])
    async def test_purge_not_the_beyond_min_connections(self, event_loop, mocker, min_connections):

        # Mock everything, we will be calling it by hand
        get_running_loop = Mock()
        call_later = Mock()
        asyncio_patched = mocker.patch("emcache.connection_pool.asyncio")
        asyncio_patched.get_running_loop = get_running_loop
        get_running_loop.return_value.call_later = call_later

        max_connections = min_connections
        connection_pool = ConnectionPool(
            ("localhost", 11211), max_connections, min_connections, 60, None, lambda _: _, False, False, None
        )

        # we add N min expired connections that must not be removed since they are the minimum ones
        expired_connections = [Mock() for _ in range(min_connections)]
        for expired_connection in expired_connections:
            connection_pool._unused_connections.append(expired_connection)
            connection_pool._connections_last_time_used[expired_connection] = time.monotonic() - 61

        connection_pool._total_connections = min_connections

        # Run the purge
        connection_pool._purge_unused_connections()

        # Check that the expired has not been removed the minimum connections
        for expired_connection in expired_connections:
            assert expired_connection in connection_pool._unused_connections
            assert expired_connection in connection_pool._connections_last_time_used

        assert connection_pool.total_connections == min_connections

        # test specific attributes of the metrics
        assert connection_pool.metrics().cur_connections == min_connections
        assert connection_pool.metrics().connections_purged == 0

    async def test_purge_connections_disabled(self, event_loop, mocker):
        get_running_loop = Mock()
        call_later = Mock()
        asyncio_patched = mocker.patch("emcache.connection_pool.asyncio")
        asyncio_patched.get_running_loop = get_running_loop
        get_running_loop.return_value.call_later = call_later

        _ = ConnectionPool(("localhost", 11211), 1, 1, None, None, lambda _: _, False, False, None)

        # check that the mock wiring was done correctly by at least observing
        # the call to the `get_running_loop`
        assert get_running_loop.called

        # With a None the call_later should not be called
        assert not call_later.called

    async def test_create_and_update_connection_timestamp(self, event_loop, mocker, not_closed_connection):
        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(return_value=not_closed_connection))

        now = time.monotonic()

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, 60, None, lambda _: _, False, False, None)

        # Create a new connection and use it
        connection_context = connection_pool.create_connection_context()
        async with connection_context as _:
            # Check that the first timestamp saved is the one that was used
            # during the creation time.
            assert connection_pool._connections_last_time_used[not_closed_connection] > now
            creation_timestamp = connection_pool._connections_last_time_used[not_closed_connection]

        # Finally when the connection is relased back to the pool the timestamp
        # is also upated.
        assert connection_pool._connections_last_time_used[not_closed_connection] > creation_timestamp

    async def test_remove_from_purge_tracking_connection_with_error(self, event_loop, mocker):
        connection = Mock()
        mocker.patch("emcache.connection_pool.create_protocol", AsyncMock(return_value=connection))

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, 60, None, lambda _: _, False, False, None)

        # Create a new connection and use it
        connection_context = connection_pool.create_connection_context()
        with pytest.raises(Exception):
            async with connection_context as _:
                raise Exception()

        connection.close.assert_called()
        assert connection not in connection_pool._connections_last_time_used
        assert connection not in connection_pool._unused_connections

    async def test_unhealthy_healthy(self, event_loop, mocker, not_closed_connection):
        mocker.patch("emcache.connection_pool.asyncio.sleep", AsyncMock())

        # Simulate as many timeouts as many errors would need to happen for
        # reaching the max retries till the connection pool is declared unhealthy
        errors = [asyncio.TimeoutError] * DECLARE_UNHEALTHY_CONNECTION_POOL_AFTER_RETRIES

        mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=errors + [not_closed_connection]),
        )

        cb_mock = Mock()

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, 1.0, cb_mock, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        await task

        # check that cb for telling that the connection pool became unhealthy and later on healty
        cb_mock.assert_has_calls([call(False), call(True)])

    async def test_unhealthy_one_event(self, event_loop, mocker, not_closed_connection):
        # Check that unhealthy status is reported once and only once

        mocker.patch("emcache.connection_pool.asyncio.sleep", AsyncMock())

        # Simulate twice of the errors that would need to happen for
        # reaching unhealthy status.
        errors = [asyncio.TimeoutError] * (DECLARE_UNHEALTHY_CONNECTION_POOL_AFTER_RETRIES * 2)

        mocker.patch(
            "emcache.connection_pool.create_protocol",
            AsyncMock(side_effect=errors),
        )

        cb_mock = Mock()

        connection_pool = ConnectionPool(("localhost", 11211), 1, 1, None, 1.0, cb_mock, False, False, None)

        async def coro(connection_context):
            async with connection_context as _:
                pass

        connection_context = connection_pool.create_connection_context()

        # wait for a new connection available in another task
        task = event_loop.create_task(coro(connection_context))

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=0.01)

        # check that was called once the unhealth report
        cb_mock.assert_called_once_with(False)


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

        connection_pool.release_connection.assert_called_with(connection, exc=None)

    async def test_release_connection_at_exit_with_exception_when_error(self):
        class MyContext(BaseConnectionContext):
            async def __aenter__(self):
                pass

        exc = Exception()
        connection_pool = Mock()
        connection = Mock()
        with pytest.raises(Exception):
            async with MyContext(connection_pool, connection, None):
                raise exc

        connection_pool.release_connection.assert_called_with(connection, exc=exc)


class TestConnectionContext:
    async def test_return_connection(self):
        connection_pool = Mock()
        connection = Mock()
        async with ConnectionContext(connection_pool, connection, None) as connection_returned:
            assert connection_returned is connection


class TestWaitingForAConnectionContext:
    async def test_wait_for_a_connection(self, event_loop):

        connection = Mock()
        connection_pool = Mock()
        connection_pool.acquire_connection.return_value = connection
        waiter = event_loop.create_future()

        async def coro():
            async with WaitingForAConnectionContext(connection_pool, None, waiter) as connection_returned:
                assert connection_returned is connection

        task = event_loop.create_task(coro())
        waiter.set_result(connection)
        await task
