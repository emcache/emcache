.. emcache documentation master file, created by
   sphinx-quickstart on Thu May 21 22:20:26 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to emcache's documentation!
===================================

A high performance asynchronous Python client for Memcached with full batteries included

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Installing
----------

- ``pip install emcache``

Basic Usage
-----------

Following snippet shows what would be the basic stuff for creating a client, through the :meth:`emcache.create_client` factory and how the objected returned can be used
later for performing :meth:`emcache.Client.get` and :meth:`emcache.Client.set` operations.

Take look to the following sections for understanding what are the different parameters supported by the :meth:`emcache.create_client` factory and what commands are
provided by the :class:`emcache.Client` object.

.. code-block:: python

    import asyncio
    import emcache
    async def main():
        client = await emcache.create_client([('localhost', 11211)])
        await client.set(b'key', b'value')
        item = await client.get(b'key')
        print(item.value)
    asyncio.run(main())
    b'value'

Contents
--------

.. toctree::

  client
  operations
  advanced_topics
