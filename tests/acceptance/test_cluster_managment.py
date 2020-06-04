import pytest

pytestmark = pytest.mark.asyncio


class TestClusterManagment:
    async def test_nodes(self, client, memcached_address_1, memcached_address_2):
        assert client.cluster_managment().nodes() == [memcached_address_1, memcached_address_2]

    async def test_healthy_nodes(self, client, memcached_address_1, memcached_address_2):
        assert client.cluster_managment().healthy_nodes() == [memcached_address_1, memcached_address_2]

    async def test_unhealthy_nodes(self, client, memcached_address_1, memcached_address_2):
        assert client.cluster_managment().unhealthy_nodes() == []
