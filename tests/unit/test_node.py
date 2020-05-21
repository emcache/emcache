import pytest

from emcache.node import Node

pytestmark = pytest.mark.asyncio


@pytest.fixture
def connection_pool(mocker):
    return mocker.patch("emcache.node.ConnectionPool")


class TestNode:
    async def test_host_and_port_properties(self, connection_pool):
        node = Node("localhost", 11211, 1, 60, 5)
        assert node.host == "localhost"
        assert node.port == 11211

    async def test_str(self, connection_pool):
        node = Node("localhost", 11211, 1, 60, 5)
        assert str(node) == "<Node host=localhost port=11211>"
        assert repr(node) == "<Node host=localhost port=11211>"

    async def test_connection_pool(self, connection_pool):
        Node("localhost", 11211, 1, 60, 5)
        connection_pool.assert_called_with("localhost", 11211, 1, 60, 5)
