import asyncio

import pytest

from fastcache import Client

pytestmark = pytest.mark.asyncio


async def test_timeout():
    unreachable_target = "0.0.0.1"
    client = Client(unreachable_target, 11211, timeout=0.1)
    with pytest.raises(asyncio.TimeoutError):
        await client.get(b"key")
