import asyncio
import time

import pytest

from fastcache import Client

pytestmark = pytest.mark.asyncio


class TestCacheClient:
    async def test_get_and_set(self):
        client = Client("localhost", 11211)
        value = str(time.time()).encode()
        await client.set(b"key", value)
        value_retrieved = await client.get(b"key")

        assert value == value_retrieved

    async def test_timeout(self):
        unreachable_target = "0.0.0.1"
        client = Client(unreachable_target, 11211, timeout=0.1)
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")
