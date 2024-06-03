# MIT License
# Copyright (c) 2020-2024 Pau Freixes

from unittest.mock import AsyncMock, Mock

import pytest

from emcache.node import MemcachedHostAddress, Node

pytestmark = pytest.mark.asyncio


@pytest.fixture
def connection_pool(mocker):
    return mocker.patch("emcache.node.ConnectionPool")


class TestMemcachedHostAddress:
    def test_attributes(self):
        memcached_node = MemcachedHostAddress("localhost", 11211)
        assert memcached_node.address == "localhost"
        assert memcached_node.port == 11211


class TestNode:
    async def test_properties(self, connection_pool, memcached_address_1):
        node = Node(memcached_address_1, 1, 1, 60, 5, lambda _: _, False, False, None)
        assert node.host == "localhost"
        assert node.port == 11211
        assert node.memcached_host_address == memcached_address_1

    async def test_str(self, connection_pool, memcached_address_1):
        node = Node(memcached_address_1, 1, 1, 60, 5, lambda _: _, False, False, None)
        assert str(node) == "<Node host=localhost port=11211 closed=False>"
        assert repr(node) == "<Node host=localhost port=11211 closed=False>"

    async def test_connection_pool(self, connection_pool, memcached_address_1):
        node = Node(memcached_address_1, 1, 1, 60, 5, lambda _: _, False, False, None)
        connection_pool.assert_called_with(
            memcached_address_1.address,
            memcached_address_1.port,
            1,
            1,
            60,
            5,
            node._on_connection_pool_healthy_status_change_cb,
            False,
            False,
            None,
        )

    async def test_close(self, mocker, memcached_address_1):
        connection_pool = Mock()
        connection_pool.close = AsyncMock()
        mocker.patch("emcache.node.ConnectionPool", return_value=connection_pool)
        node = Node(memcached_address_1, 1, 1, 60, 5, lambda _: _, False, False, None)
        await node.close()
        connection_pool.close.assert_called()

    async def test_connection_pool_healthiness_propagation(self, connection_pool, memcached_address_1):
        cb_mock = Mock()
        node = Node(memcached_address_1, 1, 1, 60, 5, cb_mock, False, False, None)
        node._on_connection_pool_healthy_status_change_cb(True)
        cb_mock.assert_called_with(node, True)
