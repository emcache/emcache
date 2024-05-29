import asyncio
import sys

import pytest

from emcache import create_client

pytestmark = pytest.mark.asyncio


class TestAuth:
    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth(self, memcached_address_4, auth_username_password):
        client = await create_client([memcached_address_4])

        with pytest.raises((asyncio.TimeoutError, TimeoutError)):
            await client.get(b"key")

        assert await client.auth(memcached_address_4, *auth_username_password) is None
        assert await client.get(b"key") is None

        await client.close()
