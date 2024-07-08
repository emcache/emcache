emcache
#######

A high performance asynchronous Python client for `Memcached <https://memcached.org/>`_ with full batteries included

.. image:: https://readthedocs.org/projects/emcache/badge/?version=latest
  :target: https://emcache.readthedocs.io/en/latest/?badge=latest

.. image:: https://github.com/emcache/emcache/workflows/CI/badge.svg
  :target: https://github.com/emcache/emcache/workflows/CI/badge.svg

.. image:: https://github.com/emcache/emcache/workflows/PyPi%20release/badge.svg
  :target: https://github.com/emcache/emcache/workflows/PyPi%20release/badge.svg

Emcache stands on the giant's shoulders and implements most of the characteristics that are desired for a Memcached client based
on the experience of other Memcached clients, providing the following main characteristics:

- Support for many Memcached hosts, distributing traffic around them by using the `Rendezvous hashing <https://emcache.readthedocs.io/en/latest/advanced_topics.html#hashing-algorithm>`_ algorithm.
- Support for different commands and different flag behaviors like ``noreply``, ``exptime`` or ``flags``.
- Support for SSL/TLS protocol.
- Support for SASL authentication by ASCII protocol.
- Support for `autodiscovery <https://emcache.readthedocs.io/en/latest/client.html#autodiscovery>`_, which should work with AWS and GCP memcached clusters.
- Adaptative `connection pool <https://emcache.readthedocs.io/en/latest/advanced_topics.html#connection-pool>`_, which increases the number of connections per Memcache host depending on the traffic.
- `Node healthiness <https://emcache.readthedocs.io/en/latest/advanced_topics.html#healthy-and-unhealthy-nodes>`_ traceability and an optional flag for disabling unhealthy for participating in the commands.
- Metrics for `operations and connections <https://emcache.readthedocs.io/en/latest/cluster_managment.html#connection-pool-metrics>`_, send them to your favourite TS database for knowing how the Emcache driver is behaving.
- Listen to the most significant `cluster events <https://emcache.readthedocs.io/en/latest/advanced_topics.html#cluster-events>`_, for example for knowing when a node has been marked as unhealthy.
- Speed, Emcache is fast. See the benchmark section.

Usage
==========

For installing

.. code-block:: bash

    pip install emcache

The following snippet shows the minimal stuff that would be needed for creating a new client and saving a new key and retrieving later the value.

.. code-block:: python

    import asyncio
    import emcache
    async def main():
        client = await emcache.create_client([emcache.MemcachedHostAddress('localhost', 11211)])
        await client.set(b'key', b'value')
        item = await client.get(b'key')
        print(item.value)
        await client.close()
    asyncio.run(main())

Emcache has currently support, among many of them, for the following commands:

- **get** Used for retrieving a specific key.
- **gets** Cas version that returns also the case token of a specific key.
- **get_many** Many keys get version.
- **gets_many** Many keys + case token gets version.
- **gat** Used retrieving a specific key if exists and update expiration time(Get and Touch).
- **gats** Cas version that retrieving a specific key if exists and update expiration time(Get and Touch with Cas).
- **gat_many** Many keys gat version.
- **gats_many** Many keys + case token gats version.
- **set** Set a new key and value
- **add** Add a new key and value, if and only if it does not exist.
- **replace** Update a value of a key, if and only if the key does exist.
- **append** Append a value to the current one for a specific key, if and only if the key does exist.
- **prepend** Prepend a value to the current one for a specific key, if and only if the key does exist.
- **cas** Update a value for a key if and only if token as provided matches with the ones stored in the Memcached server.
- **version** Version string of this server.
- **flush_all** Its effect is to invalidate all existing items immediately (by default) or after the expiration specified.
- **delete** The command allows for explicit deletion of items.
- **touch** The command is used to update the expiration time of an existing item without fetching it.
- **increment/decrement** Commands are used to change data for some item in-place, incrementing or decrementing it.
- **cache_memlimit** This command allow set in runtime cache memory limit.
- **stats** Show a list of required statistics about the server, depending on the arguments.
- **verbosity** Command control STDOUT/STDERR info, choose level and look logging memcached.

Take a look at the documentation for getting a list of all of the `operations <https://emcache.readthedocs.io/en/latest/operations.html>`_ that are currently supported.

Some of the commands have support for the following behavior flags:

- ``noreply`` for storage commands like **set** we do not wait for an explicit response from the Memcached server. Sacrifice the explicit ack from the Memcached server for speed.
- ``flags`` for storage we can save an int16 value that can be retrieved later on by fetch commands.
- ``exptime`` for storage commands this provides a way of configuring an expiration time, once that time is reached keys will be automatically evicted by the Memcached server

For more information about usage, `read the docs <https://emcache.readthedocs.io/en/latest/>`_.


Benchmarks
===========

The following table shows how fast - operations per second - Emcache can be compared to the other two Memcached Python clients,
`aiomcache <https://github.com/aio-libs/aiomcache>`_ and `pymemcache <https://github.com/pinterest/pymemcache>`_.
For that specific benchmark two nodes were used, one for the client and one for the Memcached server, using 32 TCP connections
and using 32 concurrent Asyncio tasks - threads for the use case of Pymemcache. For Emcache and Aiomcache
`uvloop <https://github.com/MagicStack/uvloop>`_ was used as a default loop.

In the first part of the benchmark, the client tried to run as mucha **set** operations it could, and in a second step the same was
done but using **get** operations.

+------------------------+---------------+---------------+-------------------+--------------------+------------------+
| Client                 | Concurrency   | Sets opS/sec  | Sets latency AVG  |  Gets opS/sec      | Gets latency AVG |
+========================+===============+===============+===================+====================+==================+
| aiomcache              |            32 |         33872 |           0.00094 |              34183 |          0.00093 |
+------------------------+---------------+---------------+-------------------+--------------------+------------------+
| pymemcache             |            32 |         32792 |           0.00097 |              32961 |          0.00096 |
+------------------------+---------------+---------------+-------------------+--------------------+------------------+
| emcache                |            32 |         49410 |           0.00064 |              49212 |          0.00064 |
+------------------------+---------------+---------------+-------------------+--------------------+------------------+
| emcache (autobatching) |            32 |         49410 |           0.00064 |              89052 |          0.00035 |
+------------------------+---------------+---------------+-------------------+--------------------+------------------+

Emcache performed better than the other two implementations reaching almost 50K ops/sec for get and set operations. One autobatching is used
it can boost the throughtput x2 (more info about autobatching below)

Another benchmark was performed for comparing how each implementation will behave in case of having to deal with more than 1 node, a new
benchmark was performed with different cluster sizes but using the same methodology as the previous test by first, performing as many set
operations it could and later as many get operations it could. For this specific use test with Aiomemcahce could not be used since it
does not support multiple nodes.

+-------------+-------------+---------------+---------------+------------------+--------------+------------------+
| Client      | Concurrency | Memcahed Nodes| Sets opS/sec  | Sets latency AVG | Gets opS/sec | Gets latency AVG |
+=============+=============+===============+===============+==================+==============+==================+
| pymemcache  |          32 |             2 |         21260 |          0.00150 |        21583 |          0.00148 |
+-------------+-------------+---------------+---------------+------------------+--------------+------------------+
| emcache     |          32 |             2 |         42245 |          0.00075 |        48079 |          0.00066 |
+-------------+-------------+---------------+---------------+------------------+--------------+------------------+
| pymemcache  |          32 |             4 |         15334 |          0.00208 |        15458 |          0.00207 |
+-------------+-------------+---------------+---------------+------------------+--------------+------------------+
| emcache     |          32 |             4 |         39786 |          0.00080 |        47603 |          0.00067 |
+-------------+-------------+---------------+---------------+------------------+--------------+------------------+
| pymemcache  |          32 |             8 |          9903 |          0.00323 |         9970 |          0.00322 |
+-------------+-------------+---------------+---------------+------------------+--------------+------------------+
| emcache     |          32 |             8 |         42167 |          0.00075 |        46472 |          0.00068 |
+-------------+-------------+---------------+---------------+------------------+--------------+------------------+

The addition of new nodes did not add almost degradation for Emcache, in the last test with 8 nodes Emcache reached 42K
get ops/sec and 46K set ops/sec. On the other hand, Pymemcached suffered substantial degradation making Emcache ~x5 times.
faster.

Autobatching
============

Autobatching provides you a way for fetching multiple keys using a single command, batching happens transparently behind the scenes
without bothering the caller.

For start using the autobatching feature you must provide the parameter `autobatching` as True, hereby all usages of the `get` and `gets` 
command will send batched requests behind the scenes.

Get´s are piled up until the next loop iteration. Once the next loop iteration is reached all get´s are transmitted using the
same Memcached operation.

Autobatching can boost up the throughput of your application x2/x3.

Development
===========

Clone the repository and its murmur3 submodule

.. code-block:: bash

    git clone --recurse-submodules git@github.com:emcache/emcache

Compile murmur3

.. code-block:: bash

    pushd vendor/murmur3
    make static
    popd

Install emcache with dev dependencies

.. code-block:: bash

    make install-dev

Testing
===========

Run docker containers, add read write privileges

.. code-block:: bash

    docker compose up -d
    docker exec memcached_unix1 sh -c "chmod a+rw /tmp/emcache.test1.sock"
    docker exec memcached_unix2 sh -c "chmod a+rw /tmp/emcache.test2.sock"

Run tests

.. code-block:: bash

    make test
