Cluster Managment
-----------------

Emcache provides a public interface for making different cluster opeations, by calling the method :meth:`emcache.Client.cluster_managment` an instance of a :class:`emcache.ClusterManagment`
will be returned, the instance returned will use behind the scenes the cluster - for example nodes and configuration - that were provided for creating the :class:`CLient` instance.

Following example shows how an instance of :class:`emcache.ClusterManagment` can be retrieved:

.. code-block:: python

    client = await emcache.create_client(
        [
            emcache.MemcachedHostAddress('localhost', 11211),
            emcache.MemcachedHostAddress('localhost', 11212)
        ]
    )
    client.cluster_managment().nodes()
    await client.close()


We might be interested on provide an intermediate factory for creating a client cache that will retrieve the cluster managment instance for using it later, for example:

.. code-block:: python

    async def create_client_cache() -> emcache.Client:
        global cache_cluster_managment
        client = await emcache.create_client(
            [
                emcache.MemcachedHostAddress('localhost', 11211),
                emcache.MemcachedHostAddress('localhost', 11212)
            ]
        )
        cache_cluster_managment = client.cluster_managment()
        return client

:class:`emcache.ClusterManagment` provides the following methods:

- :meth:`emcache.ClusterManagment.nodes` Returns the list of nodes that belong to the cluster, where each node is a :class:`emcache.MemcachedHostAddress`.
- :meth:`emcache.ClusterManagment.healthy_nodes` Returns the list of nodes that belong to the cluster that are considered healthy, where each node is a :class:`emcache.MemcachedHostAddress`. Take a look to
  the advanced options chapter and more specifically at the Healty and Unhealthy nodes for understanding what node healthiness means.
- :meth:`emcache.ClusterManagment.unhealthy_nodes` Returns the list of nodes that belong to the cluster that are considered unhealthy, where each node is a :class:`emcache.MemcachedHostAddress`. Take a look to
  the advanced options chapter and more specifically at the Healty and Unhealthy nodes for understanding what node healthiness means.
- :meth:`emcache.ClusterManagment.connection_pool_metrics` Returns the most important metrics that have been observed per each connection pool, take a look to the following section for understanding what
  metrics are being reported.

Connection pool metrics
^^^^^^^^^^^^^^^^^^^^^^^

The :meth:`emcache.ClusterManagment.connection_pool_metrics` method returns the main metrics observed by each connection pool of each of the cluster nodes. A dictionary having as
a key the :class:`emcache.MemcachedHostAddress` class instances representing each node, and as a value an instance of :class:`emcache.ConnectionPoolMetrics`. The metrics that are
returned as attributes of this class are:

- **curr_connections** Tells you how many connections are oppened at that specific moment.
- **connections_created** Tells you how many connections have been created until now.
- **connections_created_with_error** Tells you how many connections have been created but ended up by having an exception during the creation time.
- **connections_purged** Tells you how many connections have been purged.
- **connections_closed** Tells you how many connections have been closed.
- **operations_executed** Tells you how many operations have been executed.
- **operations_executed_with_error** Tells you how many operations have been executed but ended up by having an exception during the execution.
- **operations_waited** Tells you how many operations were delayed becausew they had to wait for an available connection.
- **create_connection_avg** Tells you what's the average time that creating a connection took, using the last 100 observed values.
- **create_connection_p50** Tells you what's the percentil 50 time for creating a connection, using the last 100 observed values.
- **create_connection_p99** Tells you what's the percentil 99 time for creating a connection, using the last 100 observed values.
- **create_connection_upper** Tells you what's the worst time that creating a connection took, using the last 100 observed values.

These metrics can be push to a time series database for monitoring the execution of the Emcache driver, the user will need to take care of calculating the deltas, if the user is intereseted on them, of each historical value since historical values are accumulated values.

When autobatching is enabled, each count for **operations_executed**, **operations_executed_with_error** and **operations_waited** could imply multiple keys, since autobatching uses a single command for retrieveing multiple keys.
