# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import logging
import time
from collections import deque
from copy import copy
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .protocol import MemcacheAsciiProtocol, create_protocol

logger = logging.getLogger(__name__)

CREATE_CONNECTION_MAX_BACKOFF = 60.0
DECLARE_UNHEALTHY_CONNECTION_POOL_AFTER_RETRIES = 3


_MAX_CREATE_CONNECTION_LATENCIES_OBSERVED = 100


@dataclass
class ConnectionPoolMetrics:
    """Provides basic metrics for understanding how the connection pool has
    behaved historically and currently.
    """

    # total connections that are currently opened.
    cur_connections: int

    # historical values until for how many
    # connections have been opened, how many of them
    # failed, how many connections were purged and finally
    # how many connections were closed.
    connections_created: int
    connections_created_with_error: int
    connections_purged: int
    connections_closed: int

    # historical values until now for how many
    # operations have been executed, how many of them failed
    # with an exception, and how many of them needed to wait
    # for an available connection
    operations_executed: int
    operations_executed_with_error: int
    operations_waited: int

    # Create connection latencies measured from the last,
    # if there are, 100 observed values.
    create_connection_avg: Optional[float]
    create_connection_p50: Optional[float]
    create_connection_p99: Optional[float]
    create_connection_upper: Optional[float]


class ConnectionPool:

    _waiters: deque
    _unused_connections: deque
    _total_connections: int
    _max_connections: int
    _healthy: bool
    _node_on_healthy_status_change_cb: Callable[[bool], None]
    _loop: asyncio.AbstractEventLoop
    _creating_connection_task: Optional[asyncio.Task]
    _create_connection_in_progress: bool
    _create_connection_latencies: List[float]
    _connections_last_time_used: Dict[MemcacheAsciiProtocol, float]
    _purge_unused_connections_after: Optional[int]
    _total_purged_connections: int
    _purge_timer_handler: Optional[asyncio.TimerHandle]
    _connection_timeout: Optional[float]
    _metrics: ConnectionPoolMetrics
    _closed: bool
    _ssl: bool
    _ssl_verify: bool
    _ssl_extra_ca: Optional[str]

    def __init__(
        self,
        host: str,
        port: int,
        max_connections: int,
        min_connections: int,
        purge_unused_connections_after: Optional[float],
        connection_timeout: Optional[float],
        on_healthy_status_change_cb: Callable[[bool], None],
        ssl: bool,
        ssl_verify: bool,
        ssl_extra_ca: Optional[str],
    ):
        if max_connections < 1:
            raise ValueError("max_connections must be higher than 0")
        if min_connections < 0:
            raise ValueError("min_connections must be higher or equal than 0")
        if min_connections > max_connections:
            raise ValueError("min_connections must be lower or equal than max_connections")

        self._closed = False

        self._metrics = ConnectionPoolMetrics(
            cur_connections=0,
            connections_created=0,
            connections_created_with_error=0,
            connections_purged=0,
            connections_closed=0,
            operations_executed=0,
            operations_executed_with_error=0,
            operations_waited=0,
            create_connection_avg=None,
            create_connection_p50=None,
            create_connection_p99=None,
            create_connection_upper=None,
        )

        # A connection pool starts always in a healthy state
        self._healthy = True
        self._on_healthy_status_change_cb = on_healthy_status_change_cb

        self._host = host
        self._port = port
        self._loop = asyncio.get_running_loop()
        self._connection_timeout = connection_timeout

        # attributes used for handling connections and waiters
        self._total_connections = 0
        self._max_connections = max_connections
        self._min_connections = min_connections
        self._waiters = deque()
        self._unused_connections = deque(maxlen=max_connections)

        # attributes used for creating new connections
        self._creating_connection_task = None
        self._creating_connection = False
        self._create_connection_latencies = []

        # attributes used for purging connections
        self._connections_last_time_used = {}
        self._purge_unused_connections_after = purge_unused_connections_after
        if purge_unused_connections_after is not None:
            self._purge_timer_handler = self._loop.call_later(
                self._purge_unused_connections_after, self._purge_unused_connections
            )
        else:
            self._purge_timer_handler = None

        # attributes for ssl configuration
        self._ssl = ssl
        self._ssl_verify = ssl_verify
        self._ssl_extra_ca = ssl_extra_ca

        self._maybe_new_connection_if_current_is_lower_than_min()

    def __str__(self):
        return (
            f"<ConnectionPool host={self._host} port={self._port}"
            + f" total_connections={self._total_connections}"
            + f" min_connections={self._min_connections}"
            + f" max_connections={self._max_connections} closed={self._closed}>"
        )

    def __repr__(self):
        return str(self)

    def _close_connection(self, connection):
        """Performs all of the operations needed for closing a connection."""
        connection.close()
        self._metrics.connections_closed += 1

        if connection in self._unused_connections:
            self._unused_connections.remove(connection)

        if connection in self._connections_last_time_used:
            del self._connections_last_time_used[connection]

        self._total_connections -= 1

    def _purge_unused_connections(self):
        """Iterate over all of the connections and see which ones have not
        been used recently and if its the case close and remove them.
        """
        now = time.monotonic()
        for connection, last_time_used in self._connections_last_time_used.copy().items():
            if last_time_used + self._purge_unused_connections_after > now:
                continue

            if self._total_connections <= self._min_connections:
                # Once we reach the minimum number of connections or even lower we
                # anymore.
                break

            self._close_connection(connection)
            self._metrics.connections_purged += 1
            logger.info(f"{self} Connection purged")

            # we purge one connection per loop iteration
            break

        self._loop.call_later(self._purge_unused_connections_after, self._purge_unused_connections)

    def _wakeup_next_waiter_or_append_to_unused(self, connection):
        self._connections_last_time_used[connection] = time.monotonic()

        waiter_found = None
        for waiter in reversed(self._waiters):
            if not waiter.done():
                waiter_found = waiter
                break

        if waiter_found is not None:
            waiter_found.set_result(connection)
            self._waiters.remove(waiter_found)
        else:
            self._unused_connections.append(connection)

    async def _create_new_connection(self, backoff=None, retries=None) -> None:
        """Creates a new connection in background, and once its ready
        adds it to the pool.

        If connection fails a backoff strategy is started till which will
        a totally failure node the task will remain trying forever every
        CREATE_CONNECTION_MAX_BACKOFF seconds.
        """
        error = False
        try:
            start = time.monotonic()
            connection = await create_protocol(
                self._host,
                self._port,
                ssl=self._ssl,
                ssl_verify=self._ssl_verify,
                ssl_extra_ca=self._ssl_extra_ca,
                timeout=self._connection_timeout,
            )
            elapsed = time.monotonic() - start

            # record time elapsed and keep only the last N observed values
            self._create_connection_latencies.append(elapsed)
            self._create_connection_latencies = self._create_connection_latencies[
                -_MAX_CREATE_CONNECTION_LATENCIES_OBSERVED:
            ]

            self._connections_last_time_used[connection] = time.monotonic()
            self._total_connections += 1
            self._wakeup_next_waiter_or_append_to_unused(connection)
            if not self._healthy:
                logger.info(f"{self} healthy again")
                self._healthy = True
                self._on_healthy_status_change_cb(self._healthy)
            self._metrics.connections_created += 1
            self._creating_connection = False
            logger.info(f"{self} new connection created")
        except asyncio.TimeoutError:
            logger.warning(f"{self} new connection could not be created, it timed out!")
            error = True
        except OSError as exc:
            logger.warning(f"{self} new connection could not be created, an error ocurred {exc}")
            error = True
        except asyncio.CancelledError:
            logger.info(f"{self} create connection stopped, connection pool is closing")
        finally:
            if error:
                self._metrics.connections_created_with_error += 1

                if backoff is None:
                    backoff = 1.0
                    retries = 1
                else:
                    backoff = min(CREATE_CONNECTION_MAX_BACKOFF, backoff * 2)
                    retries += 1

                if (
                    self._total_connections == 0
                    and self._healthy
                    and retries >= DECLARE_UNHEALTHY_CONNECTION_POOL_AFTER_RETRIES
                ):
                    logger.warning(f"{self} marked as unhealthy due to no connections availables")
                    self._healthy = False
                    self._on_healthy_status_change_cb(self._healthy)

                logging.info(f"{self} New connection retry in {backoff} seconds")
                await asyncio.sleep(backoff)
                await self._create_new_connection(backoff=backoff, retries=retries)

    def _maybe_new_connection_if_current_is_lower_than_min(self):
        if self._total_connections < self._min_connections:
            self._maybe_new_connection()

    def _maybe_new_connection(self) -> None:
        if self._creating_connection is False and self._total_connections < self._max_connections:

            def _keep_checking(fut: asyncio.Future):
                self._maybe_new_connection_if_current_is_lower_than_min()

            self._creating_connection = True
            self._creating_connection_task = self._loop.create_task(self._create_new_connection())
            self._creating_connection_task.add_done_callback(_keep_checking)

    async def close(self):
        """Close any active background task and close all connections"""
        # Theoretically as it is being implemented, the client must guard that
        # the connection pool close method is only called once yes or yes.
        assert self._closed is False

        self._closed = True

        if self._purge_timer_handler:
            self._purge_timer_handler.cancel()

        if self._creating_connection_task:
            self._creating_connection_task.cancel()

            try:
                await self._creating_connection_task
            except asyncio.CancelledError:
                pass

        while self._unused_connections:
            connection = self._unused_connections.pop()
            self._close_connection(connection)

    def create_connection_context(self) -> "BaseConnectionContext":
        """Returns a connection context that might provide a connection
        ready to be used, or a future connection ready to be used.

        Behind the scenes will try to make grow the pool when there
        are no connections available.
        """
        # Try to get an unused and none closed connection, otherwise
        # closed conections need to be removed.
        unused_connections = self._unused_connections
        while len(unused_connections) > 0:
            connection = unused_connections.pop()
            if connection.closed() is False:
                self._connections_last_time_used[connection] = time.monotonic()
                return ConnectionContext(self, connection, None)

            self._close_connection(connection)

        waiter = self._loop.create_future()
        self._waiters.append(waiter)

        self._maybe_new_connection()

        self._metrics.operations_waited += 1
        return WaitingForAConnectionContext(self, None, waiter)

    # Below methods are used by the _BaseConnectionContext and derivated classes.

    def release_connection(self, connection: MemcacheAsciiProtocol, *, exc: Exception = None):
        """Returns back to the pool a connection."""
        if self._closed:
            logger.debug(f"{self} Closing connection")
            self._close_connection(connection)
        elif exc or connection.closed():
            logger.warning(f"{self} Closing connection and removing from the pool due to exception {exc}")
            self._close_connection(connection)
            self._maybe_new_connection_if_current_is_lower_than_min()
            self._metrics.operations_executed_with_error += 1
        else:
            self._wakeup_next_waiter_or_append_to_unused(connection)
            self._metrics.operations_executed += 1

    def remove_waiter(self, waiter: asyncio.Future):
        "" "Remove a specifici waiter" ""
        self._waiters.remove(waiter)

    def metrics(self) -> ConnectionPoolMetrics:
        metrics = copy(self._metrics)

        # current values are updated at read time
        metrics.cur_connections = self.total_connections

        # calculate the create connection latencies only if there
        # are latencies observed, otherwise leave the default value
        # which should be None.
        if self._create_connection_latencies:
            latencies = sorted(self._create_connection_latencies)
            metrics.create_connection_avg = sum(latencies) / len(latencies)
            metrics.create_connection_p50 = latencies[int((50 * len(latencies) / 100))]
            metrics.create_connection_p99 = latencies[int((99 * len(latencies) / 100))]
            metrics.create_connection_upper = latencies[-1]

        return metrics

    @property
    def total_connections(self):
        return self._total_connections


class BaseConnectionContext:
    """Base class for providing connection contexts, see the derivated
    ones for the two different use cases.

    Base class provides the close method for returning back the connection
    to the pool.
    """

    _connection_pool: ConnectionPool
    _connection: Optional[MemcacheAsciiProtocol]
    _waiter: Optional[asyncio.Future]

    __slots__ = ("_connection_pool", "_connection", "_waiter")

    def __init__(
        self,
        connection_pool: ConnectionPool,
        connection: Optional[MemcacheAsciiProtocol],
        waiter: Optional[asyncio.Future],
    ) -> None:
        self._connection_pool = connection_pool
        self._connection = connection
        self._waiter = waiter

    async def __aenter__(self) -> MemcacheAsciiProtocol:
        raise NotImplementedError

    async def __aexit__(self, fexc_type, exc, tb) -> None:
        self._connection_pool.release_connection(self._connection, exc=exc)


class ConnectionContext(BaseConnectionContext):
    """Context used when there is a ready connection to be used."""

    async def __aenter__(self) -> MemcacheAsciiProtocol:
        return self._connection


class WaitingForAConnectionContext(BaseConnectionContext):
    """Context used when there is no a ready connection to be used. This will
    wait till a connection is given back to the loop and the waiter is being
    woken up."""

    async def __aenter__(self) -> MemcacheAsciiProtocol:
        try:
            self._connection = await self._waiter
        except asyncio.CancelledError:
            try:
                connection = self._waiter.result()
            except asyncio.CancelledError:
                # Waiter was cancelled before he could get a
                # connection, we need to remove it
                self._connection_pool.remove_waiter(self._waiter)
            else:
                # Waiter was cancelled right after wakeup. We need to
                # release the connection.  Since this is a client-side
                # cancellation, we can safely set exc=None for the
                # connetion to be reused
                self._connection_pool.release_connection(connection, exc=None)
            raise
        return self._connection
