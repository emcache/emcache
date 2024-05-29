import sys

import pytest

from emcache import create_client

pytestmark = pytest.mark.asyncio


class TestAuth:
    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth(self, memcached_address_4, auth_username_password, key_generation):
        client = await create_client([memcached_address_4])

        assert await client.auth(memcached_address_4, *auth_username_password) is None
        await client.close()
