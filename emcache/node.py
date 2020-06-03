import asyncio
import logging
from dataclasses import dataclass
from typing import Callable, Optional

from .connection_pool import BaseConnectionContext, ConnectionPool

logger = logging.getLogger(__name__)


@dataclass
class MemcachedHostAddress:
    """ Data class for identifying univocally a Memcached host. """

    address: str
    port: int


class Node:
    """ Node class is in charge of providing a connection for communicating
    with the Memcached node, also it holds any attribute related to the
    Memcached node.
    """

    _memcached_host_address: MemcachedHostAddress
    _healthy: bool
    _on_healthy_status_change_cb: Callable[[bool], None]
    _connection_pool: ConnectionPool
    _loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        memcached_host_address: MemcachedHostAddress,
        max_connections: int,
        purge_unused_connections_after: Optional[float],
        connection_timeout: Optional[float],
        on_healthy_status_change_cb: Callable[["Node", bool], None],
    ) -> None:

        # A connection pool starts always in a healthy state
        self._healthy = True
        self._on_healthy_status_change_cb = on_healthy_status_change_cb

        self._memcached_host_address = memcached_host_address
        self._loop = asyncio.get_running_loop()
        self._connection_pool = ConnectionPool(
            self.host,
            self.port,
            max_connections,
            purge_unused_connections_after,
            connection_timeout,
            self._on_connection_pool_healthy_status_change_cb,
        )
        logger.debug(f"{self} new node created")

    def __str__(self) -> str:
        return f"<Node host={self.host} port={self.port}>"

    def __repr__(self) -> str:
        return str(self)

    def _on_connection_pool_healthy_status_change_cb(self, healthy: bool):
        # The healthiness of the node depends only to the healthiness of
        # the connection pool
        self._healthy = healthy

        if self._healthy:
            logger.info(f"{self} Connection pool reports a healthy status")
        else:
            logger.warning(f"{self} Connection pool resports an unhealthy status")

        self._on_healthy_status_change_cb(self, self._healthy)

    def connection(self) -> BaseConnectionContext:
        return self._connection_pool.create_connection_context()

    @property
    def host(self) -> str:
        return self._memcached_host_address.address

    @property
    def port(self) -> int:
        return self._memcached_host_address.port

    @property
    def memcached_host_address(self) -> MemcachedHostAddress:
        return self._memcached_host_address
