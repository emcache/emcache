# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import os
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
async def memcached_address_3():
    return MemcachedHostAddress("localhost", 11213)


@pytest.fixture
async def memcached_address_4():
    return MemcachedHostAddress("localhost", 11214)


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


@pytest.fixture
def auth_userpass():
    with open(os.path.join(os.path.dirname(__file__), "acceptance", "data", "auth_pwd.txt"), "rb") as f:
        return f.readline().rstrip().split(b":")
