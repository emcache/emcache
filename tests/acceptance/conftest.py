# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import os
import time
from pathlib import Path

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


@pytest.fixture()
async def client(node_addresses, event_loop):
    client = await create_client(node_addresses, timeout=2.0)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture()
async def authed_client(event_loop, auth_userpass):
    username, password = auth_userpass
    client = await create_client(
        [MemcachedHostAddress("localhost", 11214)], timeout=1.0, username=username, password=password
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
    with open(Path(os.path.dirname(__file__)) / "data" / "auth_pwd.txt", "r") as f:
        return f.readline().rstrip().split(":")
