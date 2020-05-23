Client creation
---------------

Client creation is done exclusively through the asynchronous factory :meth:`emcache.create_client`, this function returns a valid :class:`emcache.Client` object that is used
later for making all of the operations, like :meth:`emcache.Client.get` or :meth:`emcache.Client.set`.

By default :meth:`emcache.create_client` only askes for one parameter, the list of the Memcached host addresses, hosts, and ports. Other parameters would be
configured to their default value when they are not provided.

For example

.. code-block:: python

    client = await emcache.create_client(
        [('localhost', 11211), ('localhost', 11212)]
    )

The previous example would return a :class:`emcache.Client` object instance that will perform the operations to two different Nodes, depending on the outcome of the hashing algorithm,
take a look to the advanced topics section and specifically to the Hashing section for understanding how operations are being routed to the different nodes.

When no other parameters are provided, the following keyword arguments are configured to the following default values:

- **timeout** Configured to 1.0, meaning that any operation that might take more than 1 second would be considered timed out and an :exc:`asyncio.TimeoutError` would be triggered
  For disabling timeouts at operation level a `None` value can be provided.
- **max_connections** Configured to 2, maximum number of TCP connections that would be opened per node. Consider configure that number according to the maximum number of concurrent
  clients that you might have and the impact that these connections might have for the Memcached server. Take look to the advanced topics section, and specifically to the 
  Connection pool section.
- **purge_unused_connections_after** By default disabled using `None`. If you want to purge - close - connections that haven't been used for a while configure here the threshold of seconds 
  that a connection would be allowed to keep unused before closing it, for example, 60.0.
- **connection_timeout** By default configured to 5 seconds, meaning that any attempt of creating a new connection that might take more than 5 seconds would be considered timed out.
  For disabling that time out give a `None` value.

Example of a client creation that would purge unused connections

.. code-block:: python

    client = await emcache.create_client(
        [('localhost', 11211), ('localhost', 11212)],
        purge_unused_connections_after=60.0
    )
