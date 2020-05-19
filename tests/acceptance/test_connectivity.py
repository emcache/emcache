import asyncio

import pytest

from fastcache import Client

pytestmark = pytest.mark.asyncio

UNREACHABLE_HOST = "0.0.0.1"


async def test_timeout():
    client = Client([(UNREACHABLE_HOST, 11211)], timeout=0.1)
    with pytest.raises(asyncio.TimeoutError):
        await client.get(b"key")


async def test_timeout_multiple_nodes(memcached_address_1, memcached_address_2):
    # Use two available hosts and one unreachable, everything
    # would need to be cancelled
    client = Client([memcached_address_1, memcached_address_2, (UNREACHABLE_HOST, 11211)], timeout=0.1)
    keys = [str(i).encode() for i in range(100)]
    with pytest.raises(asyncio.TimeoutError):
        await client.get_many(keys)
