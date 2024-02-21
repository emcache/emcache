# MIT License
# Copyright (c) 2020-2024 Pau Freixes

from emcache._cython import cyemcache


class TestRendezvousNode:
    def test_node_is_public_attribute(self):
        node = object()
        rendezvous_node = cyemcache.RendezvousNode("host", 11211, node)
        assert rendezvous_node.node is node


class TestNodeSelection:
    def test_one_node(self):
        # Having only one node no matter the key value
        # would need to return always the same node
        node = object()
        rendezvous_node = cyemcache.RendezvousNode("host", 11211, node)
        node_selected = cyemcache.node_selection(b"foo", [rendezvous_node])
        assert node_selected is node

    def test_three_nodes(self):
        node1 = object()
        node2 = object()
        node3 = object()
        rendezvous_node1 = cyemcache.RendezvousNode("host1", 11211, node1)
        rendezvous_node2 = cyemcache.RendezvousNode("host2", 11211, node2)
        rendezvous_node3 = cyemcache.RendezvousNode("host3", 11211, node3)

        list_of_rendezvous_nodes = [rendezvous_node1, rendezvous_node2, rendezvous_node3]

        keys_per_node = {node1: [], node2: [], node3: []}

        # keys should be ~ evenly distributed through all of the nodes.
        for i in range(1000):
            key = str(i).encode()
            node_selected = cyemcache.node_selection(key, list_of_rendezvous_nodes)
            keys_per_node[node_selected].append(key)

        # Just check that the number of keys per node are close
        # to the expected ones.
        assert len(keys_per_node[node1]) > 300
        assert len(keys_per_node[node2]) > 300
        assert len(keys_per_node[node3]) > 300

        # Operations for the same keys should be idempotent when same
        # keys are used.
        next_round_keys_per_node = {node1: [], node2: [], node3: []}

        for i in range(1000):
            key = str(i).encode()
            node_selected = cyemcache.node_selection(key, list_of_rendezvous_nodes)
            next_round_keys_per_node[node_selected].append(key)

        assert keys_per_node[node1] == next_round_keys_per_node[node1]
        assert keys_per_node[node2] == next_round_keys_per_node[node2]
        assert keys_per_node[node3] == next_round_keys_per_node[node3]


class TestNodesSelection:
    def test_one_node(self):
        # Having only one node no matter the key value
        # would need to return always the same node
        node = object()
        rendezvous_node = cyemcache.RendezvousNode("host", 11211, node)
        keys_per_node = cyemcache.nodes_selection([b"foo"], [rendezvous_node])
        assert keys_per_node == {node: [b"foo"]}

    def test_three_nodes(self):
        node1 = "node1"
        node2 = "node2"
        node3 = "node3"
        rendezvous_node1 = cyemcache.RendezvousNode("host1", 11211, node1)
        rendezvous_node2 = cyemcache.RendezvousNode("host2", 11211, node2)
        rendezvous_node3 = cyemcache.RendezvousNode("host3", 11211, node3)

        list_of_rendezvous_nodes = [rendezvous_node1, rendezvous_node2, rendezvous_node3]

        # generate a list of keys.
        keys = [str(i).encode() for i in range(1000)]
        keys_per_node = cyemcache.nodes_selection(keys, list_of_rendezvous_nodes)

        # only the three nodes configured expected as keys
        assert sorted(keys_per_node.keys()) == sorted([node1, node2, node3])

        # Just check that the three nodes have had some keys assigned, close to be
        # evently distributed
        assert len(keys_per_node[node1]) > 300
        assert len(keys_per_node[node2]) > 300
        assert len(keys_per_node[node3]) > 300

        # Operations for the same keys should be idempotent when same
        # keys are used.
        next_round_keys_per_node = cyemcache.nodes_selection(keys, list_of_rendezvous_nodes)

        assert keys_per_node[node1] == next_round_keys_per_node[node1]
        assert keys_per_node[node2] == next_round_keys_per_node[node2]
        assert keys_per_node[node3] == next_round_keys_per_node[node3]
