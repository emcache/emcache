# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import time

import pytest

from emcache import MemcachedHostAddress, MemcachedUnixSocketPath, create_client


@pytest.fixture(
    params=[
        pytest.param(
            [MemcachedHostAddress("localhost", 11211), MemcachedHostAddress("localhost", 11212)], id="tcp_client"
        ),
        pytest.param(
            [MemcachedUnixSocketPath("/tmp/emcache.test1.sock"), MemcachedUnixSocketPath("/tmp/emcache.test2.sock")],
            id="unix_client",
        ),
    ]
)
def node_addresses(request):
    return request.param


@pytest.fixture
async def client(node_addresses, event_loop):
    client = await create_client(node_addresses, timeout=2.0)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def client2(node_addresses, event_loop):
    client = await create_client([MemcachedHostAddress("localhost", 11211)], timeout=2.0)
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
