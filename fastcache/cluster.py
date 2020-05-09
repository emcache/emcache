import logging
from typing import List, Tuple

from .node import Node
from ._cython import cyfastcache


logger = logging.getLogger(__name__)


class Cluster:
    """Cluster class is used for keeping all of the nodes together, and
    for providing any interface that needs to have visibility of all of
    those nodes that belong to a cluster.

    As an example, the node selection for a sepcific key, is being provided
    by the Cluster class.
    """

    _nodes: List[Node]
    _rdz_nodes: List[cyfastcache.RendezvousNode]

    def __init__(self, node_addresses: List[Tuple[str, int]]) -> None:

        # Create nodes and configure them to be used by the Rendezvous
        # hashing.
        self._nodes = [
            Node(host, port) for host, port in node_addresses]
        self._rdz_nodes = [
            cyfastcache.RendezvousNode(node.host, node.port, node) for node in self._nodes]

        logger.debug(f"Cluster configured with {len(self._nodes)} nodes")

    def pick_node(self, key: bytes) -> Node:
        """Return the most appropiate node for the given key.

        Node selected will be resolved by the Rendezvous hashing
        algorithm, which will be idempotent when the cluster nodes
        do not change.
        """
        return cyfastcache.node_selection(key, self._rdz_nodes)
