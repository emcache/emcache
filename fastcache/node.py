import asyncio
import logging
from collections import deque
from typing import Dict, List, Optional, Tuple

from ._cython import cyfastcache
from .connection_pool import BaseConnectionContext, ConnectionPool
from .protocol import MemcacheAsciiProtocol, create_protocol

# Has been observed that values higher than 32 do not provide
# a significant increase on the OPS/sec, while them could have
# a negative impact on the latency.
DEFAULT_MAX_CONNECTIONS = 32

logger = logging.getLogger(__name__)


class Node:
    """ Node class is in charge of providing a connection for communicating
    with the Memcached node, also it holds any attribute related to the
    Memcached node.
    """

    _host: str
    _port: int
    _connection_pool: ConnectionPool
    _loop: asyncio.AbstractEventLoop

    def __init__(self, host: str, port: int, max_connections: int = DEFAULT_MAX_CONNECTIONS) -> None:
        self._host = host
        self._port = port
        self._loop = asyncio.get_running_loop()
        self._connection_pool = ConnectionPool(host, port, max_connections)
        logger.info(f"{self} new node created")

    def __str__(self) -> str:
        return f"<Node host={self._host} port={self._port}>"

    def __repr__(self) -> str:
        return str(self)

    def connection(self) -> BaseConnectionContext:
        return self._connection_pool.create_connection_context()

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port
