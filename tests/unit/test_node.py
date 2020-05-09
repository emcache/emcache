import pytest

from fastcache.node import DEFAULT_MAX_CONNECTIONS, Node

pytestmark = pytest.mark.asyncio


@pytest.fixture
def connection_pool(mocker):
    return mocker.patch("fastcache.node.ConnectionPool")


class TestNode:
    async def test_attributes(self):
        node = Node("localhost", 11211)
        assert node.host == "localhost"
        assert node.port == 11211
        assert str(node) == "<Node host=localhost port=11211>"
        assert repr(node) == "<Node host=localhost port=11211>"

    async def test_connection_pool(self, connection_pool):
        Node("localhost", 11211)
        connection_pool.assert_called_with("localhost", 11211, DEFAULT_MAX_CONNECTIONS)

    async def test_max_connections(self, connection_pool):
        Node("localhost", 11211, max_connections=16)
        connection_pool.assert_called_with("localhost", 11211, 16)
