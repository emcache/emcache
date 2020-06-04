from unittest.mock import Mock, call

import pytest

from emcache.base import ClusterEvents
from emcache.client_errors import ClusterNoAvailableNodes
from emcache.cluster import Cluster, _ClusterManagment
from emcache.node import MemcachedHostAddress

pytestmark = pytest.mark.asyncio


@pytest.fixture
def memcached_host_address_1():
    return MemcachedHostAddress("localhost", 11211)


@pytest.fixture
def memcached_host_address_2():
    return MemcachedHostAddress("localhost", 11212)


@pytest.fixture
def node1(memcached_host_address_1):
    node1 = Mock()
    node1.host = memcached_host_address_1.address
    node1.port = memcached_host_address_1.port
    node1.memcached_host_address = memcached_host_address_1
    return node1


@pytest.fixture
def node2(memcached_host_address_2):
    node2 = Mock()
    node2.host = memcached_host_address_2.address
    node2.port = memcached_host_address_2.port
    node2.memcached_host_address = memcached_host_address_2
    return node2


@pytest.fixture
def cluster_with_one_node(mocker, node1, memcached_host_address_1):
    mocker.patch("emcache.cluster.Node", return_value=node1)
    return Cluster([memcached_host_address_1], 1, 60, 5, None, False)


@pytest.fixture
def cluster_with_two_nodes(mocker, node1, node2, memcached_host_address_1, memcached_host_address_2):
    mocker.patch("emcache.cluster.Node", side_effect=[node1, node2])
    return Cluster([memcached_host_address_1, memcached_host_address_2], 1, 60, 5, None, False)


@pytest.fixture
def cluster_with_one_node_purge_unhealthy(mocker, node1, memcached_host_address_1):
    mocker.patch("emcache.cluster.Node", return_value=node1)
    return Cluster([memcached_host_address_1], 1, 60, 5, None, True)


@pytest.fixture
def cluster_with_two_nodes_purge_unhealthy(mocker, node1, node2, memcached_host_address_1, memcached_host_address_2):
    mocker.patch("emcache.cluster.Node", side_effect=[node1, node2])
    return Cluster([memcached_host_address_1, memcached_host_address_2], 1, 60, 5, None, True)


class Test_ClusterManagment:
    def test_nodes(self, node1, node2):
        cluster = Mock()
        cluster.nodes = [node1, node2]
        cluster_managment = _ClusterManagment(cluster)
        assert cluster_managment.nodes() == [node1.memcached_host_address, node2.memcached_host_address]

    def test_healthy_nodes(self, node1, node2):
        cluster = Mock()
        cluster.healthy_nodes = [node1, node2]
        cluster_managment = _ClusterManagment(cluster)
        assert cluster_managment.healthy_nodes() == [node1.memcached_host_address, node2.memcached_host_address]

    def test_unhealthy_nodes(self, node1, node2):
        cluster = Mock()
        cluster.unhealthy_nodes = [node1, node2]
        cluster_managment = _ClusterManagment(cluster)
        assert cluster_managment.unhealthy_nodes() == [node1.memcached_host_address, node2.memcached_host_address]


class TestCluster:
    def test_invalid_number_of_nodes(self):
        with pytest.raises(ValueError):
            Cluster([], 1, 60, 5, None, False)

    async def test_node_initialization(self, event_loop, mocker, memcached_host_address_1, memcached_host_address_2):
        mocker.patch("emcache.cluster.cyemcache")
        node_class = mocker.patch("emcache.cluster.Node")
        cluster = Cluster([memcached_host_address_1, memcached_host_address_2], 1, 60, 5, None, False)

        node_class.assert_has_calls(
            [
                call(memcached_host_address_1, 1, 60, 5, cluster._on_node_healthy_status_change_cb),
                call(memcached_host_address_2, 1, 60, 5, cluster._on_node_healthy_status_change_cb),
            ]
        )

    async def test_cluster_managment(self, mocker, memcached_host_address_1):
        mocker.patch("emcache.cluster.cyemcache")
        mocker.patch("emcache.cluster.Node")

        cluster_managment = Mock()
        cluster_managment_class = mocker.patch("emcache.cluster._ClusterManagment", return_value=cluster_managment)
        cluster = Cluster([memcached_host_address_1], 1, 60, 5, None, False)

        # Check that the initialization was done using the right parameters
        cluster_managment_class.assert_called_with(cluster)

        # Check that cluster returns the instance of cluster managment that is expected
        assert cluster.cluster_managment is cluster_managment

    async def test_pick_node(self, cluster_with_one_node, node1):
        node = cluster_with_one_node.pick_node(b"key")
        assert node == node1

    async def test_pick_nodes(self, cluster_with_one_node, node1):
        nodes = cluster_with_one_node.pick_nodes([b"key", b"key2"])
        assert list(nodes.keys())[0] == node1
        assert list(nodes.values())[0] == [b"key", b"key2"]

    async def test_unhealthy_not_purge_node(self, cluster_with_two_nodes, node1, node2):
        # Report that node2 is unhealthy
        cluster_with_two_nodes._on_node_healthy_status_change_cb(node2, False)

        # traffic is still send to both nodes
        keys = [str(i).encode() for i in range(100)]
        nodes = cluster_with_two_nodes.pick_nodes(keys)

        assert node1 in nodes
        assert node2 in nodes

        # check all node properties
        assert cluster_with_two_nodes.nodes == [node1, node2]
        assert cluster_with_two_nodes.healthy_nodes == [node1]
        assert cluster_with_two_nodes.unhealthy_nodes == [node2]

    async def test_unhealthy_purge_node(self, cluster_with_two_nodes_purge_unhealthy, node1, node2):
        # Report that node2 is unhealthy
        cluster_with_two_nodes_purge_unhealthy._on_node_healthy_status_change_cb(node2, False)

        keys = [str(i).encode() for i in range(100)]
        nodes = cluster_with_two_nodes_purge_unhealthy.pick_nodes(keys)

        # traffic is send to onnly the healhty node
        assert node1 in nodes
        assert node2 not in nodes

        # All traffic should be send to the healhty node
        assert nodes[node1] == keys

        # check all node properties
        assert cluster_with_two_nodes_purge_unhealthy.nodes == [node1, node2]
        assert cluster_with_two_nodes_purge_unhealthy.healthy_nodes == [node1]
        assert cluster_with_two_nodes_purge_unhealthy.unhealthy_nodes == [node2]

    async def test_unhealthy_pick_node_no_available_nodes(self, cluster_with_one_node_purge_unhealthy, node1):
        # Report that node_1 is unhealthy
        cluster_with_one_node_purge_unhealthy._on_node_healthy_status_change_cb(node1, False)

        with pytest.raises(ClusterNoAvailableNodes):
            cluster_with_one_node_purge_unhealthy.pick_node(b"key")

    async def test_unhealthy_pick_nodes_no_available_nodes(self, cluster_with_one_node_purge_unhealthy, node1):
        # Report that node_1 is unhealthy
        cluster_with_one_node_purge_unhealthy._on_node_healthy_status_change_cb(node1, False)

        with pytest.raises(ClusterNoAvailableNodes):
            cluster_with_one_node_purge_unhealthy.pick_nodes([b"key", b"key2"])

    async def test_cluster_events(self, mocker, memcached_host_address_1):
        class MyClusterEvents(ClusterEvents):
            def __init__(self):
                self.on_healhty_node_called = 0
                self.on_unhealhty_node_called = 0

            def on_node_healthy(self, memcached_host_address):
                assert memcached_host_address == memcached_host_address_1
                self.on_healhty_node_called += 1

            def on_node_unhealthy(self, memcached_host_address):
                assert memcached_host_address == memcached_host_address_1
                self.on_unhealhty_node_called += 1

        cluster_events = MyClusterEvents()

        node1 = Mock()
        node1.host = memcached_host_address_1.address
        node1.port = memcached_host_address_1.port
        node1.memcached_host_address = memcached_host_address_1
        mocker.patch("emcache.cluster.Node", return_value=node1)
        cluster = Cluster([memcached_host_address_1], 1, 60, 5, cluster_events, False)

        # Set the node unhealhty and healthy again
        cluster._on_node_healthy_status_change_cb(node1, False)
        cluster._on_node_healthy_status_change_cb(node1, True)

        # Check that hooks were called
        assert cluster_events.on_healhty_node_called == 1
        assert cluster_events.on_unhealhty_node_called == 1
