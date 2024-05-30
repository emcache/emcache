import sys

import pytest

from emcache import MemcachedHostAddress, create_client
from emcache.client_errors import AuthenticationError

pytestmark = pytest.mark.asyncio


class TestAuth:
    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth(self, memcached_address_1, memcached_address_4, auth_userpass):
        client = await create_client([memcached_address_1])
        await client.get(b"key")
        await client.close()

        username, password = auth_userpass
        client = await create_client([memcached_address_4], username=username.decode(), password=password.decode())
        await client.get(b"key")
        await client.close()

    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth_with_errors(self):
        with pytest.raises(AuthenticationError):
            client = await create_client([MemcachedHostAddress("localhost", 11214)], timeout=2.0)
            await client.close()
        with pytest.raises(AuthenticationError):
            await create_client([MemcachedHostAddress("localhost", 11214, username="wrong")], timeout=2.0)
            await client.close()
        with pytest.raises(AuthenticationError):
            await create_client([MemcachedHostAddress("localhost", 11214, password="wrong")], timeout=2.0)
            await client.close()
        with pytest.raises(AuthenticationError):
            await create_client([MemcachedHostAddress("localhost", 11214, "wrong", "wrong")], timeout=2.0)
            await client.close()
