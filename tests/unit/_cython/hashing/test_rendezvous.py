import pytest

from fastcache._cython import cyfastcache


class TestRendezvousNode:

    def test_node_is_public_attribute(self):
        node = object()
        rendezvous_node = cyfastcache.RendezvousNode("host", 11211, node)
        assert rendezvous_node.node is node


class TestNodeSelection:

    def test_one_node(self):
        # Having only one node no matter the key value
        # would need to return always the same node
        node = object()
        rendezvous_node = cyfastcache.RendezvousNode("host", 11211, node)
        node_selected = cyfastcache.node_selection(b"foo", [rendezvous_node])
        assert node_selected is node

    def test_three_nodes(self):
        node1 = object()
        node2 = object()
        node3 = object()
        rendezvous_node1 = cyfastcache.RendezvousNode("host1", 11211, node1)
        rendezvous_node2 = cyfastcache.RendezvousNode("host2", 11211, node2)
        rendezvous_node3 = cyfastcache.RendezvousNode("host3", 11211, node3)

        list_of_rendezvous_nodes = [
            rendezvous_node1,
            rendezvous_node2,
            rendezvous_node3
        ]

        keys_per_node = {
            node1: [],
            node2: [],
            node3: []
        }

        # keys should be ~ evenly distributed through all of the nodes.
        for i in range(1000):
            key = str(i).encode()
            node_selected = cyfastcache.node_selection(
                key,
                list_of_rendezvous_nodes
            )
            keys_per_node[node_selected].append(key)

        # Just check that the number of keys per node are close
        # to the expected ones.
        assert len(keys_per_node[node1]) > 300
        assert len(keys_per_node[node2]) > 300
        assert len(keys_per_node[node3]) > 300

        # Operations for the same keys should be idempotent when same
        # keys are used.
        next_round_keys_per_node = {
            node1: [],
            node2: [],
            node3: []
        }

        for i in range(1000):
            key = str(i).encode()
            node_selected = cyfastcache.node_selection(
                key,
                list_of_rendezvous_nodes
            )
            next_round_keys_per_node[node_selected].append(key)

        assert keys_per_node[node1] == next_round_keys_per_node[node1]
        assert keys_per_node[node2] == next_round_keys_per_node[node2]
        assert keys_per_node[node3] == next_round_keys_per_node[node3]
