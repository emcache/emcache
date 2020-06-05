import asyncio
import logging
import time
from collections import deque
from copy import copy
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from .protocol import MemcacheAsciiProtocol, create_protocol

logger = logging.getLogger(__name__)

CREATE_CONNECTION_MAX_BACKOFF = 60.0
DECLARE_UNHEALTHY_CONNECTION_POOL_AFTER_RETRIES = 3


@dataclass
class ConnectionPoolMetrics:
    """ Provides basic metrics for understanding how the connection pool has
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
    _connections_last_time_used: Dict[MemcacheAsciiProtocol, float]
    _purge_unused_connections_after: Optional[int]
    _total_purged_connections: int
    _connection_timeout: Optional[float]
    _metrics: ConnectionPoolMetrics

    def __init__(
        self,
        host: str,
        port: int,
        max_connections: int,
        purge_unused_connections_after: Optional[float],
        connection_timeout: Optional[float],
        on_healthy_status_change_cb: Callable[[bool], None],
    ):
        if max_connections < 1:
            raise ValueError("max_connections must be higher than 0")

        self._metrics = ConnectionPoolMetrics(
            cur_connections=0,
            connections_created=0,
            connections_created_with_error=0,
            connections_purged=0,
            connections_closed=0,
            operations_executed=0,
            operations_executed_with_error=0,
            operations_waited=0,
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
        self._waiters = deque()
        self._unused_connections = deque(maxlen=max_connections)

        # attributes used for creating new connections
        self._creating_connection_task = None
        self._creating_connection = False

        # attributes used for purging connections
        self._connections_last_time_used = {}
        self._purge_unused_connections_after = purge_unused_connections_after
        if purge_unused_connections_after is not None:
            self._loop.call_later(self._purge_unused_connections_after, self._purge_unused_connections)

        # At least one connection needs to be available
        self._maybe_new_connection()

    def __str__(self):
        return f"<ConnectionPool host={self._host} port={self._port} total_connections={self._total_connections}>"

    def __repr__(self):
        return str(self)

    def _close_connection(self, connection):
        """ Performs all of the operations needed for closing a connection. """
        connection.close()
        self._metrics.connections_closed += 1

        if connection in self._unused_connections:
            self._unused_connections.remove(connection)

        if connection in self._connections_last_time_used:
            del self._connections_last_time_used[connection]

        self._total_connections -= 1

    def _purge_unused_connections(self):
        """ Iterate over all of the connections and see which ones have not
        been used recently and if its the case close and remove them.
        """
        now = time.monotonic()
        for connection, last_time_used in self._connections_last_time_used.copy().items():
            if last_time_used + self._purge_unused_connections_after > now:
                continue

            if self._total_connections == 1:
                # If there is only one connection we don't purgue it since
                # the connection pool always tries to have at least one connection.
                break

            self._close_connection(connection)
            self._metrics.connections_purged += 1
            logger.info(f"{self} Connection purged")

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
        """ Creates a new connection in background, and once its ready
        adds it to the poool.

        If connection fails a backoff strategy is started till which will
        a totally failure node the task will remain trying forever every
        CREATE_CONNECTION_MAX_BACKOFF seconds.
        """
        error = False
        try:
            connection = await create_protocol(self._host, self._port, timeout=self._connection_timeout)
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

    def _maybe_new_connection(self) -> None:
        # We kick off another connection if the following conditions are meet
        # - there is still room for having more connections
        # - there is no an ongoing creation of a connection.
        # - there are waiters or there are no connections
        if (
            self._creating_connection is False
            and self._total_connections < self._max_connections
            and (len(self._waiters) > 0 or self._total_connections == 0)
        ):
            self._creating_connection = True
            self._creating_connection_task = self._loop.create_task(self._create_new_connection())

    def create_connection_context(self) -> "BaseConnectionContext":
        """ Returns a connection context that might provide a connection
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
                return ConnectionContext(self, connection, None)

            self._close_connection(connection)

        waiter = self._loop.create_future()
        self._waiters.append(waiter)

        self._maybe_new_connection()

        self._metrics.operations_waited += 1
        return WaitingForAConnectionContext(self, None, waiter)

    # Below methods are used by the _BaseConnectionContext and derivated classes.

    def release_connection(self, connection: MemcacheAsciiProtocol, *, exc: Exception = None):
        """ Returns back to the pool a connection."""
        if exc or connection.closed():
            logger.warning(f"{self} Closing connection and removing from the pool due to exception {exc}")
            self._close_connection(connection)
            self._maybe_new_connection()
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
        return metrics

    @property
    def total_connections(self):
        return self._total_connections


class BaseConnectionContext:
    """ Base class for providing connection contexts, see the derivated
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
    """ Context used when there is a ready connection to be used."""

    async def __aenter__(self) -> MemcacheAsciiProtocol:
        return self._connection


class WaitingForAConnectionContext(BaseConnectionContext):
    """ Context used when there is no a ready connection to be used. This will
    wait till a connection is given back to the loop and the waiter is being
    woken up."""

    async def __aenter__(self) -> MemcacheAsciiProtocol:
        try:
            self._connection = await self._waiter
        except asyncio.CancelledError:
            self._connection_pool.remove_waiter(self._waiter)
            raise

        return self._connection
