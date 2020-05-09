import asyncio
import logging
from collections import deque
from typing import Dict, List, Optional, Tuple

from .protocol import create_protocol, MemcacheAsciiProtocol 
from ._cython import cyfastcache


logger = logging.getLogger(__name__)


class ConnectionPool:

    _waiters: deque
    _connection_pool: deque
    _total_connections: int
    _max_connections: int
    _loop: asyncio.AbstractEventLoop
    _connection_task: Optional[asyncio.Task]
    _connection_in_progress: bool

    def __init__(self, host: str, port: int,  max_connections: int):
        self._host = host
        self._port = port
        self._total_connections = 0
        self._max_connections = max_connections
        self._waiters = deque()
        self._connection_pool = deque(maxlen=max_connections)
        self._connection_task = None
        self._creating_connection = False
        self._loop = asyncio.get_running_loop()

    def __str__(self):
        return f"<ConnectionPool host={self._host} port={self._port} total_connections={self._total_connections}>"

    def __repr__(self):
        return str(self)

    def _wakeup_next_waiter(self):
        if not self._waiters:
            return

        for waiter in reversed(self._waiters):
            if not waiter.done():
                waiter.set_result(None)
                break

    async def _create_new_connection(self) -> None:
        """ Creates a new connection in background, and once its ready
        adds it to the poool.
        """
        try:
            connection = await create_protocol(self._host, self._port)
        finally:
            self._creating_connection = False

        self._total_connections += 1
        self._connection_pool.append(connection)
        self._wakeup_next_waiter()
        logger.info(f"{self} new connection created")

    def create_connection_context(self) -> 'BaseConnectionContext':
        """ Returns a connection context that might provide a connection
        ready to be used, or a future connection ready to be used.

        Behind the scenes will try to make grow the pool when there
        are no connections available.
        """
        if len(self._connection_pool) > 0 and len(self._waiters) == 0:
            connection = self._connection_pool.pop()
            return ConnectionContext(self, connection, None)

        waiter = self._loop.create_future()
        self._waiters.append(waiter)

        # We kick off another connection if there is still room for having more connections
        # in the pool and there is no an ongoing creation of a connection.
        if self._creating_connection is False and self._total_connections < self._max_connections:
            self._creating_connection = True
            self._connection_task = self._loop.create_task(self._create_new_connection())

        return WaitingForAConnectionContext(self, None, waiter)

    # Below methods are used by the _BaseConnectionContext and derivated classes.

    def acquire_connection(self):
        """ Returns an available connection, if there is."""
        return self._connection_pool.pop()

    def release_connection(self, connection: MemcacheAsciiProtocol):
        """ Returns back to the pool a connection."""
        self._connection_pool.append(connection)
        self._wakeup_next_waiter()

    def remove_waiter(self, waiter: asyncio.Future):
        "" "Remove a specifici waiter"""
        self._waiters.remove(waiter)


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
            waiter: Optional[asyncio.Future]) -> None:
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
            await self._waiter
        finally:
            self._connection_pool.remove_waiter(self._waiter)

        self._connection = self._connection_pool.acquire_connection()
        return self._connection
