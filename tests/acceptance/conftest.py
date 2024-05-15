# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import time

import pytest

from emcache import MemcachedHostAddress, create_client


@pytest.fixture
async def memcached_address_1():
    return MemcachedHostAddress("localhost", 11211)


@pytest.fixture
async def memcached_address_2():
    return MemcachedHostAddress("localhost", 11212)


@pytest.fixture
async def client(event_loop, memcached_address_1, memcached_address_2):
    client = await create_client(
        [memcached_address_1, memcached_address_2],
        timeout=2.0,
    )
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture(scope="session")
def key_generation():
    def _():
        cnt = 0
        base = time.time()
        while True:
            yield str(base + cnt).encode()
            cnt += 1

    return _()
