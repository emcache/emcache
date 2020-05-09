import pytest

from fastcache.cluster import Cluster

pytestmark = pytest.mark.asyncio


class TestCluster:

    async def test_pick_node(self):
        cluster = Cluster([
            ('localhost', 11211)
        ])

        node = cluster.pick_node(b"key")
        assert node.host == 'localhost'
        assert node.port == 11211
