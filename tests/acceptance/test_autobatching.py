# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio

import pytest

from emcache import create_client
from emcache.default_values import DEFAULT_AUTOBATCHING_MAX_KEYS

pytestmark = pytest.mark.asyncio


class TestAutobatching:
    @pytest.fixture
    async def autobatching_client(self, event_loop, memcached_address_1, memcached_address_2):
        autobatching_client = await create_client(
            [memcached_address_1, memcached_address_2], timeout=2.0, autobatching=True
        )
        try:
            yield autobatching_client
        finally:
            await autobatching_client.close()

    @pytest.mark.parametrize("max_keys", [1, DEFAULT_AUTOBATCHING_MAX_KEYS, DEFAULT_AUTOBATCHING_MAX_KEYS * 2])
    async def test_get_multiple_keys(
        self, event_loop, key_generation, max_keys, memcached_address_1, memcached_address_2
    ):
        autobatching_client = await create_client(
            [memcached_address_1, memcached_address_2], timeout=2.0, autobatching=True, autobatching_max_keys=max_keys
        )
        keys_and_values = []
        for i in range(2):
            value = next(key_generation)
            keys_and_values.append(value)
            await autobatching_client.set(value, value)

        async def coro(key):
            return await autobatching_client.get(key)

        tasks = [event_loop.create_task(coro(key)) for key in keys_and_values]
        items = await asyncio.gather(*tasks)

        assert len(items) == 2
        assert items[0].value in keys_and_values
        assert items[1].value in keys_and_values

    async def test_get(self, event_loop, autobatching_client, key_generation):
        key_and_value = next(key_generation)
        await autobatching_client.set(key_and_value, key_and_value)
        item = await autobatching_client.get(key_and_value)
        assert item.value == key_and_value
        assert item.flags is None
        assert item.cas is None

    async def test_get_return_flags(self, event_loop, autobatching_client, key_generation):
        key_and_value = next(key_generation)
        await autobatching_client.set(key_and_value, key_and_value, flags=1)
        item = await autobatching_client.get(key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.flags == 1
        assert item.cas is None

    async def test_gets(self, event_loop, autobatching_client, key_generation):
        key_and_value = next(key_generation)
        await autobatching_client.set(key_and_value, key_and_value)
        item = await autobatching_client.gets(key_and_value)
        assert item.value == key_and_value
        assert item.flags is None
        assert item.cas is not None

    async def test_gets_return_flags(self, event_loop, autobatching_client, key_generation):
        key_and_value = next(key_generation)
        await autobatching_client.set(key_and_value, key_and_value, flags=1)
        item = await autobatching_client.gets(key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.flags == 1
        assert item.cas is not None

    async def test_not_found(self, event_loop, autobatching_client):
        item = await autobatching_client.get(b"miss")
        assert item is None
