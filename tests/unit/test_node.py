# MIT License
# Copyright (c) 2020-2024 Pau Freixes

from unittest.mock import AsyncMock, Mock

import pytest

from emcache import MemcachedHostAddress
from emcache.node import Node

pytestmark = pytest.mark.asyncio


@pytest.fixture
def memcached_host_address():
    return MemcachedHostAddress("localhost", 11211)


@pytest.fixture
def connection_pool(mocker):
    return mocker.patch("emcache.node.ConnectionPool")


class TestMemcachedHostAddress:
    def test_attributes(self):
        memcached_node = MemcachedHostAddress("localhost", 11211)
        assert memcached_node.address == "localhost"
        assert memcached_node.port == 11211


class TestNode:
    async def test_properties(self, connection_pool, memcached_host_address):
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _, False, False, None, None, None)
        assert node.host == "localhost"
        assert node.port == 11211
        assert node.memcached_host_address == memcached_host_address

    async def test_str(self, connection_pool, memcached_host_address):
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _, False, False, None, None, None)
        assert str(node) == "<Node host=localhost port=11211 closed=False>"
        assert repr(node) == "<Node host=localhost port=11211 closed=False>"

    async def test_connection_pool(self, connection_pool, memcached_host_address):
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _, False, False, None, None, None)
        connection_pool.assert_called_with(
            MemcachedHostAddress(memcached_host_address.address, memcached_host_address.port),
            1,
            1,
            60,
            5,
            node._on_connection_pool_healthy_status_change_cb,
            False,
            False,
            None,
            None,
            None,
        )

    async def test_close(self, mocker, memcached_host_address):
        connection_pool = Mock()
        connection_pool.close = AsyncMock()
        mocker.patch("emcache.node.ConnectionPool", return_value=connection_pool)
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _, False, False, None, None, None)
        await node.close()
        connection_pool.close.assert_called()

    async def test_connection_pool_healthiness_propagation(self, connection_pool, memcached_host_address):
        cb_mock = Mock()
        node = Node(memcached_host_address, 1, 1, 60, 5, cb_mock, False, False, None, None, None)
        node._on_connection_pool_healthy_status_change_cb(True)
        cb_mock.assert_called_with(node, True)
