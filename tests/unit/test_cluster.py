from unittest.mock import call

import pytest

from emcache.cluster import Cluster, MemcachedHostAddress

pytestmark = pytest.mark.asyncio


class TestMemcachedHostAddress:
    def test_attributes(self):
        memcached_node = MemcachedHostAddress("localhost", 11211)
        assert memcached_node.address == "localhost"
        assert memcached_node.port == 11211


class TestCluster:
    async def test_node_initialization(self, event_loop, mocker):
        mocker.patch("emcache.cluster.cyemcache")
        node_class = mocker.patch("emcache.cluster.Node")
        Cluster([MemcachedHostAddress("localhost", 11211), MemcachedHostAddress("localhost", 11212)], 1, 60, 5)

        node_class.assert_has_calls([call("localhost", 11211, 1, 60, 5), call("localhost", 11212, 1, 60, 5)])

    async def test_pick_node(self):
        cluster = Cluster([MemcachedHostAddress("localhost", 11211)], 1, 60, 5)

        node = cluster.pick_node(b"key")
        assert node.host == "localhost"
        assert node.port == 11211
