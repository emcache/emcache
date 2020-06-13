import sys

import pytest

from emcache import NotFoundIncrDecrCommandError

pytestmark = pytest.mark.asyncio


class TestIncr:
    async def test_incr(self, client, key_generation):
        key = next(key_generation)

        # incr a value for a key that does not exist must fail
        with pytest.raises(NotFoundIncrDecrCommandError):
            await client.increment(key, 1)

        # set the new key and increment the value.
        await client.set(key, b"1")
        value = await client.increment(key, 1)

        assert value == 2

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_incr_noreply(self, client, key_generation):
        key = next(key_generation)

        # set the new key and increment the value using noreply
        await client.set(key, b"1")
        value = await client.increment(key, 1, noreply=True)

        # when noreply is used a None is returned
        assert value is None

        item = await client.get(key)

        assert item.value == b"2"


class TestDecr:
    async def test_decr(self, client, key_generation):
        key = next(key_generation)

        # decr a value for a key that does not exist must fail
        with pytest.raises(NotFoundIncrDecrCommandError):
            await client.decrement(key, 1)

        # set the new key and decrement the value.
        await client.set(key, b"2")
        value = await client.decrement(key, 1)

        assert value == 1

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_decr_noreply(self, client, key_generation):
        key = next(key_generation)

        # set the new key and decrement the value using noreply
        await client.set(key, b"2")
        value = await client.decrement(key, 1, noreply=True)

        # when noreply is used a None is always returned
        assert value is None

        item = await client.get(key)

        assert item.value == b"1"
