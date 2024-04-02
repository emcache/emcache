Client creation
---------------

Client creation is done exclusively through the asynchronous factory :meth:`emcache.create_client`, this function returns a valid :class:`emcache.Client` object that is used
later for making all of the operations, like :meth:`emcache.Client.get` or :meth:`emcache.Client.set`.

By default :meth:`emcache.create_client` only askes for one parameter, the list of the Memcached host addresses, hosts, and ports. Other parameters would be
configured to their default values when they are not provided.

For example

.. code-block:: python

    client = await emcache.create_client(
        [
            emcache.MemcachedHostAddress('localhost', 11211),
            emcache.MemcachedHostAddress('localhost', 11212)
        ]
    )

The previous example would return a :class:`emcache.Client` object instance that will perform the operations to two different Nodes, depending on the outcome of the hashing algorithm.
Take a look to the advanced topics section and specifically to the Hashing section for understanding how operations are being routed to the different nodes.

When no other parameters are provided, the following keyword arguments are configured to the following default values:

- **timeout** Enabled and configured to **1.0 seconds**, meaning that any operation that might take more than 1 second would be considered timed out and an :exc:`asyncio.TimeoutError` would be triggered
  For disabling timeouts at operation level a `None` value can be provided.
- **max_connections** Configured to 2, maximum number of TCP connections that would be opened per node. Consider configure that number according to the maximum number of concurrent
  clients that you might have and the impact that these connections might have for the Memcached server. Take look to the advanced topics section, and specifically to the 
  Connection pool section.
- **min_connections** Configured to 1, minum number of TCP connections that the client will try to keep open in ideal circumstances.
  Connection pool section.
- **purge_unused_connections_after** By default enabled and conigured to **60.0 secconds**. If you do not want to purge actively - close - connections that haven't been used for a while give a `None` value.
- **connection_timeout** By default configured to **5 seconds**, meaning that any attempt of creating a new connection that might take more than 5 seconds would be considered timed out.
  For disabling that time out give a `None` value.
- **cluster_events** By default configured to `None`. Take a look to the advanced topics section, and specifically to the cluster events section.
- **purge_unhealthy_nodes** By default configured to False, if it was configured to True traffic wouldn't be send to nodes that are reporting an unhealthy status. Take a look to the advanced topics section, and specifically to the healthy and unhealhty nodes section.
- **autobatching** By default configured to False, if it was configured to True all `get` and `gets` methods will use autobatching strategy, leveraing on the `get_many` command for sending group of get´s operations in batches. This option can speed up your application x2/x3. More information take a look to the Autobatching secion within the Advanced Options section.
- **autobatching_max_keys** By default 32. Will be used only if **autobatching** is enabled. Configures the maximum number of keys to be sent in a single batch.
- **ssl** By default False. If enabled will make the connection using SSL/TLS protocol.
- **ssl_verify** By default True. If enabled, will verify if the server certificate is trustable.
- **ssl_extra_ca** By default `None`. If provided will used as an extr CA file used during the certificate verify. Use together `ssl_verify` when needed.
- **autodiscovery** By default, `False`. If enabled the client will periodically call `config get cluster` and update node list according to its output. Note, this is not a standard feature of memcached, but is necessary to
  efficiently use AWS' ElasticCache and GCP's Memorystore (and any other providers that also adopted it) when those clusters are bigger than a single node.
- **autodiscovery_poll_interval** By default, 60.0 (seconds). When autodiscovery is enabled how frequently to check for node updates.
- **autodiscovery_timeout** By default, 5.0 (seconds). The timeout for the `config get cluster` command.

Example of a client creation that would not purge unused connections

.. code-block:: python

    client = await emcache.create_client(
        [
            emcache.MemcachedHostAddress('localhost', 11211),
            emcache.MemcachedHostAddress('localhost', 11212)
        ],
        purge_unused_connections_after=None
    )


Some underlying resources are started as background tasks when the client is instantiated, these resources would need to be closed gracefully using the :meth:`emcache.Client.close` method. This method will trigger all of the job necessary for releasing these resources. The following snippet shows how this method can be used:

.. code-block:: python

    client = await emcache.create_client(
        [
            emcache.MemcachedHostAddress('localhost', 11211),
            emcache.MemcachedHostAddress('localhost', 11212)
        ]
    )

    await client.close()

Autodiscovery
^^^^^^^^^^^^^

Emcache supports autodiscovery mechanism implemented by AWS and GCP (https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.html)

Example of enabling autodiscovery (`mycluster.fnjyzo.cfg.use1.cache.amazonaws.com:11211` will be used to query other nodes
every 120s and with timeout of 10s).

.. code-block:: python

    client = await emcache.create_client(
        [
            emcache.MemcachedHostAddress('mycluster.fnjyzo.cfg.use1.cache.amazonaws.com', 11211)
        ],
        autodiscovery=True,
        autodiscovery_poll_interval=120.0,
        autodiscovery_timeout=10.0
    )

    await client.close()
