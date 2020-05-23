Advanced Topics
----------------

Hashing algorithm
^^^^^^^^^^^^^^^^^

The distribution alogithm is used for deciding which node of the cluster should deal with a specific key and it's based
on the `Rendezvous hashing <https://en.wikipedia.org/wiki/Rendezvous_hashing>`_ algorithm which is a generalized version
of the consistent hashing.

The Emcache implementation uses a constant and equal weight for all of the nodes and key hash uses an 8 bytes version of the
`murmur hash <https://en.wikipedia.org/wiki/MurmurHash>`_.

The main properties of the algorithm are:

- Idempotence, if the set of nodes does not vary, the same key would be handled by the same node again and again.
- In case of removing a node from the Cluster only the keys that were handled by the removed Node would be distributed across all of the other nodes.
- In case of an addition of a node to the Cluster, a percentage of the keys that were initially handled by one node will be handled by a different node.
  Theoretically, the number of keys affected should be equal to the percentage of the nodes added, for example by adding one node to a cluster of 10
  nodes, this should induce to have 10% of the keys routed to a different node.

One of the drawbacks of this algorithm is the performance when many nodes are used since the routing algorithm
needs to calculate the hash for a specific key for all of the nodes. This might have an impact when the size of the
cluster is about hundreds or thousands of nodes.

Connection Pool
^^^^^^^^^^^^^^^

The connection pool is the element that maintains the TCP connections opened to a specific node. Will be as many different instances of connections pools as many
nodes the cluster has.

By default the connection pool, if the default values are not overwritten, is initialized with the following characteristics:

- Create a maximum of 2 TCP connections. This can be changed by providing a different value of the ``max_connections`` keyword of the :meth:`emcache.create_client` factory.
- Not purge unused connections, meaning that connections that once created are no longer used won't be explicitly closed after a while. this can be changed
  by providing a different value of the ``purge_unused_connections_after`` keyword of the :meth:`emcache.create_client` factory.
- Give up by timeout after 5 seconds if a connection can't be created. This can be changed by providing a different value of the ``connection_timeout`` keyword
  of the :meth:`emcache.create_client` factory.

The maximum number of connections should be configured carefully, considering that connections are a limited resource that might have a noticeable impact on the
Memcached nodes. While having a limit of 32 connections might be a valid value for an environment with a few client instances, this would most likely become a to high value for an environment with a large number of instances.

Following table shows you the maximum throughput that has been achieved with a different number of connections with
a single client instance:

+------------+------------+
| Connections| Ops/sec    |
+============+============+
|          1 |      12127 |
+------------+------------+
|          2 |      19325 |
+------------+------------+
|          4 |      28721 |
+------------+------------+
|          8 |      38219 |
+------------+------------+
|         16 |      41355 |
+------------+------------+
|         32 |      49386 |
+------------+------------+
|         64 |      51410 |
+------------+------------+
|        128 |      52262 |
+------------+------------+

Any number beyond 32 TCP connections did not have a significant increase in the number of operations per second. By default, the connection pool comes configured with 2 maximum TCP connections,
which should provide in a modern CPU ~20K ops/sec.
