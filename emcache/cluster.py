import asyncio
import logging
from typing import Dict, List, Mapping, Optional, Sequence

from ._cython import cyemcache
from .base import ClusterEvents, ClusterManagment
from .client_errors import ClusterNoAvailableNodes
from .connection_pool import ConnectionPoolMetrics
from .node import MemcachedHostAddress, Node

logger = logging.getLogger(__name__)

MAX_EVENTS = 1000


class _ClusterManagment(ClusterManagment):
    _cluster: "Cluster"

    def __init__(self, cluster: "Cluster") -> None:
        self._cluster = cluster

    def nodes(self) -> List[MemcachedHostAddress]:
        """Return the nodes that belong to the cluster. """
        return [node.memcached_host_address for node in self._cluster.nodes]

    def healthy_nodes(self) -> List[MemcachedHostAddress]:
        """Return the nodes that are considered healthy. """
        return [node.memcached_host_address for node in self._cluster.healthy_nodes]

    def unhealthy_nodes(self) -> List[MemcachedHostAddress]:
        """Return the nodes that are considered unhealthy. """
        return [node.memcached_host_address for node in self._cluster.unhealthy_nodes]

    def connection_pool_metrics(self) -> Mapping[MemcachedHostAddress, ConnectionPoolMetrics]:
        """Return metrics gathered at emcache driver side for each of the
        cluster nodes for its connection pool.

        For more information about what metrics are being returned take a look
        to the `ConnectionPoolMetrics`.
        """
        return {node.memcached_host_address: node.connection_pool_metrics() for node in self._cluster.nodes}


class Cluster:
    """Cluster class is used for keeping all of the nodes together, and
    for providing any interface that needs to have visibility of all of
    those nodes that belong to a cluster.

    As an example, the node selection for a sepcific key, is being provided
    by the Cluster class.
    """

    _cluster_events: Optional[ClusterEvents]
    _purge_unhealthy_nodes: bool
    _healthy_nodes: List[Node]
    _unhelathy_nodes: List[Node]
    _rdz_nodes: List[cyemcache.RendezvousNode]
    _events_dispatcher_task: asyncio.Task
    _events: asyncio.Queue
    _closed: bool

    def __init__(
        self,
        memcached_hosts_address: Sequence[MemcachedHostAddress],
        max_connections: int,
        purge_unused_connections_after: Optional[float],
        connection_timeout: Optional[float],
        cluster_events: Optional[ClusterEvents],
        purge_unhealthy_nodes: bool,
    ) -> None:

        if not memcached_hosts_address:
            raise ValueError("Nodes can't be an empty list, at least one node has to be provided")

        self._cluster_events = cluster_events
        self._purge_unhealthy_nodes = purge_unhealthy_nodes

        # Create nodes and configure them to be used by the Rendezvous
        # hashing. By default all are placed under the healthy list, if later
        # on they report that are unhealhty and `purge_unhealthy_nodes` has been
        # configured they will be removed.
        self._unhealthy_nodes = []
        self._healthy_nodes = [
            Node(
                memcached_host_address,
                max_connections,
                purge_unused_connections_after,
                connection_timeout,
                self._on_node_healthy_status_change_cb,
            )
            for memcached_host_address in memcached_hosts_address
        ]
        self._build_rdz_nodes()
        self._cluster_managment = _ClusterManagment(self)
        self._events = asyncio.Queue(maxsize=MAX_EVENTS)
        self._events_dispatcher_task = asyncio.get_running_loop().create_task(self._events_dispatcher())
        self._closed = False
        logger.debug(f"Cluster configured with {len(self.nodes)} nodes")

    async def _events_dispatcher(self):
        logger.debug(f"Events dispatcher started")
        while True:
            try:
                coro = await self._events.get()
                await coro
            except asyncio.CancelledError:
                # if a cancellation is received we stop processing events
                break
            except Exception:
                logger.exception(f"Hook raised an exception, continuing processing events")

        logger.debug("Events dispatcher stopped")

    def _build_rdz_nodes(self):
        """ Builds the list of rdz nodes using the nodes that claim to be healhty."""
        if self._purge_unhealthy_nodes:
            nodes = self._healthy_nodes
        else:
            nodes = self._healthy_nodes + self._unhealthy_nodes

        logger.info(f"Nodes used for sending traffic: {nodes}")

        self._rdz_nodes = [cyemcache.RendezvousNode(node.host, node.port, node) for node in nodes]

    def _on_node_healthy_status_change_cb(self, node: Node, healthy: bool):
        """ CalleClose any active background task and close all TCP
        connections.

        It does not implement any gracefull close at operation level,
        if there are active operations the outcome is not predictabled
        by the node for telling that the healthy status has changed. """
        if healthy:
            assert node in self._unhealthy_nodes, "Node was not tracked by the cluster as unhealthy!"
            self._unhealthy_nodes.remove(node)
            self._healthy_nodes.append(node)
            logger.info(f"Node {node} reports a healthy status")
        else:
            assert node in self._healthy_nodes, "Node was not tracked by the cluster as healthy!"
            self._healthy_nodes.remove(node)
            self._unhealthy_nodes.append(node)
            logger.warning(f"Node {node} reports an unhealthy status")

        self._build_rdz_nodes()

        # trigger cluster events if they were provided
        if self._cluster_events:
            try:
                if healthy:
                    self._events.put_nowait(
                        self._cluster_events.on_node_healthy(self._cluster_managment, node.memcached_host_address)
                    )
                else:
                    self._events.put_nowait(
                        self._cluster_events.on_node_unhealthy(self._cluster_managment, node.memcached_host_address)
                    )
            except asyncio.QueueFull:
                logger.warning("Events can't be dispathed, queue full")

    async def close(self):
        """ Close any active background task and close all nodes """
        # Theoretically as it is being implemented, the client must guard that
        # the cluster close method is only called once yes or yes.
        assert self._closed is False

        self._closed = True

        if not self._events_dispatcher_task.done():
            self._events_dispatcher_task.cancel()

            try:
                await self._events_dispatcher_task
            except asyncio.CancelledError:
                pass

        for node in self.nodes:
            await node.close()

    def pick_node(self, key: bytes) -> Node:
        """Return the most appropiate node for the given key.

        Node selected will be resolved by the Rendezvous hashing
        algorithm, which will be idempotent when the cluster nodes
        do not change.
        """
        if len(self._rdz_nodes) == 0:
            raise ClusterNoAvailableNodes()

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
        if len(self._rdz_nodes) == 0:
            raise ClusterNoAvailableNodes()

        return cyemcache.nodes_selection(keys, self._rdz_nodes)

    def node(self, memcached_host_address: MemcachedHostAddress) -> Node:
        """Return the emcache Node that matches with a memcached_host_address."""
        for node in self.nodes:
            if node.memcached_host_address == memcached_host_address:
                return node

        raise ValueError(f"{memcached_host_address} can not be found")

    @property
    def cluster_managment(self) -> ClusterManagment:
        return self._cluster_managment

    @property
    def nodes(self) -> List[Node]:
        return self._healthy_nodes + self._unhealthy_nodes

    @property
    def healthy_nodes(self) -> List[Node]:
        return self._healthy_nodes[:]

    @property
    def unhealthy_nodes(self) -> List[Node]:
        return self._unhealthy_nodes[:]
