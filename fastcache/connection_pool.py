import asyncio
import logging
from collections import deque
from typing import Optional

from .protocol import MemcacheAsciiProtocol, create_protocol

logger = logging.getLogger(__name__)


class ConnectionPool:

    _waiters: deque
    _unused_connections: deque
    _total_connections: int
    _max_connections: int
    _loop: asyncio.AbstractEventLoop
    _connection_task: Optional[asyncio.Task]
    _connection_in_progress: bool
    _connections_waited: int

    def __init__(self, host: str, port: int, max_connections: int):
        self._host = host
        self._port = port
        self._total_connections = 0
        self._stats_connections_waited = 0
        self._max_connections = max_connections
        self._waiters = deque()
        self._unused_connections = deque(maxlen=max_connections)
        self._connection_task = None
        self._creating_connection = False
        self._loop = asyncio.get_running_loop()

    def __str__(self):
        return f"<ConnectionPool host={self._host} port={self._port} total_connections={self._total_connections}>"

    def __repr__(self):
        return str(self)

    def _wakeup_next_waiter_or_append_to_unused(self, connection):
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

    async def _create_new_connection(self) -> None:
        """ Creates a new connection in background, and once its ready
        adds it to the poool.
        """
        try:
            connection = await create_protocol(self._host, self._port)
            self._total_connections += 1
            self._wakeup_next_waiter_or_append_to_unused(connection)
            logger.info(f"{self} new connection created")
        finally:
            self._creating_connection = False

    def create_connection_context(self) -> "BaseConnectionContext":
        """ Returns a connection context that might provide a connection
        ready to be used, or a future connection ready to be used.

        Behind the scenes will try to make grow the pool when there
        are no connections available.
        """
        if len(self._unused_connections) > 0:
            connection = self._unused_connections.pop()
            return ConnectionContext(self, connection, None)

        waiter = self._loop.create_future()
        self._waiters.append(waiter)

        # We kick off another connection if there is still room for having more connections
        # in the pool and there is no an ongoing creation of a connection.
        if self._creating_connection is False and self._total_connections < self._max_connections:
            self._creating_connection = True
            self._connection_task = self._loop.create_task(self._create_new_connection())

        self._stats_connections_waited += 1
        return WaitingForAConnectionContext(self, None, waiter)

    # Below methods are used by the _BaseConnectionContext and derivated classes.

    def release_connection(self, connection: MemcacheAsciiProtocol):
        """ Returns back to the pool a connection."""
        self._wakeup_next_waiter_or_append_to_unused(connection)

    def remove_waiter(self, waiter: asyncio.Future):
        "" "Remove a specifici waiter" ""
        self._waiters.remove(waiter)

    # stats methods
    def stats_connections_waited(self) -> int:
        value = self._stats_connections_waited
        self._stats_connections_waited = 0
        return value


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
        self._connection_pool.release_connection(self._connection)


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
