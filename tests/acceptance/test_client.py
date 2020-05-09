import pytest
import time

from fastcache import Client


pytestmark = pytest.mark.asyncio


class TestCacheClient:

    async def test_get_and_set(self):
        client = Client("localhost", 11211)
        value = str(time.time()).encode()
        await client.set(b"key", value)
        value_retrieved = await client.get(b"key")

        assert value == value_retrieved
