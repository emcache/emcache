# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio

import pytest

from emcache import MemcachedHostAddress, create_client

pytestmark = pytest.mark.asyncio

UNREACHABLE_HOST = "0.0.0.1"


async def test_timeout():
    client = await create_client([MemcachedHostAddress(UNREACHABLE_HOST, 11211)], timeout=0.1)
    with pytest.raises(asyncio.TimeoutError):
        await client.get(b"key")

    await client.close()


async def test_timeout_multiple_nodes(memcached_address_1, memcached_address_2):
    # Use two available hosts and one unreachable, everything
    # would need to be cancelled
    client = await create_client(
        [memcached_address_1, memcached_address_2, MemcachedHostAddress(UNREACHABLE_HOST, 11211)], timeout=0.1
    )
    keys = [str(i).encode() for i in range(100)]
    with pytest.raises(asyncio.TimeoutError):
        await client.get_many(keys)

    await client.close()
