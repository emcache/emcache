import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from ._cython import cyemcache
from .node import Node

logger = logging.getLogger(__name__)


@dataclass
class MemcachedHostAddress:
    """ Data class for identifying univocally a Memcached host. """

    address: str
    port: int


class Cluster:
    """Cluster class is used for keeping all of the nodes together, and
    for providing any interface that needs to have visibility of all of
    those nodes that belong to a cluster.

    As an example, the node selection for a sepcific key, is being provided
    by the Cluster class.
    """

    _nodes: List[Node]
    _rdz_nodes: List[cyemcache.RendezvousNode]

    def __init__(
        self,
        node_addresses: Sequence[MemcachedHostAddress],
        max_connections: int,
        purge_unused_connections_after: Optional[float],
        connection_timeout: Optional[float],
    ) -> None:

        # Create nodes and configure them to be used by the Rendezvous
        # hashing.
        self._nodes = [
            Node(
                memcache_host_address.address,
                memcache_host_address.port,
                max_connections,
                purge_unused_connections_after,
                connection_timeout,
            )
            for memcache_host_address in node_addresses
        ]
        self._rdz_nodes = [cyemcache.RendezvousNode(node.host, node.port, node) for node in self._nodes]

        logger.debug(f"Cluster configured with {len(self._nodes)} nodes")

    def pick_node(self, key: bytes) -> Node:
        """Return the most appropiate node for the given key.

        Node selected will be resolved by the Rendezvous hashing
        algorithm, which will be idempotent when the cluster nodes
        do not change.
        """
        return cyemcache.node_selection(key, self._rdz_nodes)

    def pick_nodes(self, keys: bytes) -> Dict[Node, List[bytes]]:
        """Return the most appropiate nodes for the given keys.

        Return value is a dictionary where nodes stand for keys
        and values are the list of keys that would need to be used
        for that node.

        Nodes selected will be resolved by the Rendezvous hashing
        algorithm, which will be idempotent when the cluster nodes
        do not change.
        """
        return cyemcache.nodes_selection(keys, self._rdz_nodes)
