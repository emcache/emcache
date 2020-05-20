import time

import pytest

from emcache import Client


@pytest.fixture
async def memcached_address_1():
    return "localhost", 11211


@pytest.fixture
async def memcached_address_2():
    return "localhost", 11212


@pytest.fixture
async def client(event_loop, memcached_address_1, memcached_address_2):
    return Client([memcached_address_1, memcached_address_2])


@pytest.fixture(scope="session")
def key_generation():
    def _():
        cnt = 0
        base = time.time()
        while True:
            yield str(base + cnt).encode()
            cnt += 1

    return _()
