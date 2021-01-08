from unittest.mock import Mock

import pytest
from asynctest import CoroutineMock

from emcache.node import MemcachedHostAddress, Node

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
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _)
        assert node.host == "localhost"
        assert node.port == 11211
        assert node.memcached_host_address == memcached_host_address

    async def test_str(self, connection_pool, memcached_host_address):
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _)
        assert str(node) == "<Node host=localhost port=11211 closed=False>"
        assert repr(node) == "<Node host=localhost port=11211 closed=False>"

    async def test_connection_pool(self, connection_pool, memcached_host_address):
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _)
        connection_pool.assert_called_with(
            memcached_host_address.address,
            memcached_host_address.port,
            1,
            1,
            60,
            5,
            node._on_connection_pool_healthy_status_change_cb,
        )

    async def test_close(self, mocker, memcached_host_address):
        connection_pool = Mock()
        connection_pool.close = CoroutineMock()
        mocker.patch("emcache.node.ConnectionPool", return_value=connection_pool)
        node = Node(memcached_host_address, 1, 1, 60, 5, lambda _: _)
        await node.close()
        connection_pool.close.assert_called()

    async def test_connection_pool_healthiness_propagation(self, connection_pool, memcached_host_address):
        cb_mock = Mock()
        node = Node(memcached_host_address, 1, 1, 60, 5, cb_mock)
        node._on_connection_pool_healthy_status_change_cb(True)
        cb_mock.assert_called_with(node, True)
