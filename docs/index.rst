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

Usage
-----

Using emcache is as simple as

.. code-block:: python

    >>> import asyncio
    >>> import emcache
    >>> async def main():
    >>>     client = emcache.Client([('localhost', 11211)])
    >>>     await client.set(b'key', b'value')
    >>>     item = await client.get(b'key')
    >>>     print(item.value)
    >>> asyncio.run(main())
    b'value'
