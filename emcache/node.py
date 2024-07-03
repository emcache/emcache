# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import logging
from typing import Callable, Optional, Union

from ._address import MemcachedHostAddress, MemcachedUnixSocketPath
from .connection_pool import BaseConnectionContext, ConnectionPool, ConnectionPoolMetrics

logger = logging.getLogger(__name__)


class Node:
    """Node class is in charge of providing a connection for communicating
    with the Memcached node, also it holds any attribute related to the
    Memcached node.
    """

    _memcached_host_address: Union[MemcachedHostAddress, MemcachedUnixSocketPath]
    _healthy: bool
    _on_healthy_status_change_cb: Callable[[bool], None]
    _connection_pool: ConnectionPool
    _closed: bool

    def __init__(
        self,
        memcached_host_address: Union[MemcachedHostAddress, MemcachedUnixSocketPath],
        max_connections: int,
        min_connections: int,
        purge_unused_connections_after: Optional[float],
        connection_timeout: Optional[float],
        on_healthy_status_change_cb: Optional[Callable[["Node", bool], None]],
        ssl: bool,
        ssl_verify: bool,
        ssl_extra_ca: Optional[str],
        username: Optional[str],
        password: Optional[str],
    ) -> None:

        # A connection pool starts always in a healthy state
        self._healthy = True
        self._on_healthy_status_change_cb = on_healthy_status_change_cb

        self._memcached_host_address = memcached_host_address
        self._connection_pool = ConnectionPool(
            memcached_host_address,
            max_connections,
            min_connections,
            purge_unused_connections_after,
            connection_timeout,
            self._on_connection_pool_healthy_status_change_cb,
            ssl,
            ssl_verify,
            ssl_extra_ca,
            username,
            password,
        )
        self._closed = False
        logger.debug(f"{self} new node created")

    def __str__(self) -> str:
        if isinstance(self._memcached_host_address, MemcachedUnixSocketPath):
            return f"<Node path={self.path} closed={self._closed}>"
        return f"<Node host={self.host} port={self.port} closed={self._closed}>"

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return self.memcached_host_address.__hash__()

    def _on_connection_pool_healthy_status_change_cb(self, healthy: bool):
        # The healthiness of the node depends only to the healthiness of
        # the connection pool
        self._healthy = healthy

        if self._healthy:
            logger.info(f"{self} Connection pool reports a healthy status")
        else:
            logger.warning(f"{self} Connection pool resports an unhealthy status")

        if self._on_healthy_status_change_cb:
            self._on_healthy_status_change_cb(self, self._healthy)

    async def close(self):
        """Close any active background task and close the connection pool"""
        # Theoretically as it is being implemented, the client must guard that
        # the node close method is only called once yes or yes.
        assert self._closed is False

        self._closed = True
        await self._connection_pool.close()

    def connection(self) -> BaseConnectionContext:
        return self._connection_pool.create_connection_context()

    def connection_pool_metrics(self) -> ConnectionPoolMetrics:
        return self._connection_pool.metrics()

    @property
    def host(self) -> str:
        if isinstance(self._memcached_host_address, MemcachedUnixSocketPath):
            raise AttributeError("host not available on node using Unix socket")
        return self._memcached_host_address.address

    @property
    def port(self) -> int:
        if isinstance(self._memcached_host_address, MemcachedUnixSocketPath):
            raise AttributeError("host not available on node using Unix socket")
        return self._memcached_host_address.port

    @property
    def path(self) -> str:
        if isinstance(self._memcached_host_address, MemcachedHostAddress):
            raise AttributeError("path not available on node using TCP socket")
        return self._memcached_host_address.path

    @property
    def memcached_host_address(self) -> Union[MemcachedHostAddress, MemcachedUnixSocketPath]:
        return self._memcached_host_address
