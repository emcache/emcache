import pytest

from emcache import MemcachedHostAddress, create_client

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def memcached_unix_socket():
    return MemcachedHostAddress("/tmp/emcache.sock", 0)


class TestUnixSocket:
    async def test_create_client(self, memcached_unix_socket):
        client = await create_client([memcached_unix_socket], timeout=1.0)
        await client.set(b"key", b"1")
        assert (await client.get(b"key")).value == b"1"
        await client.close()
