import time

import pytest

from fastcache import Client


@pytest.fixture
async def client(event_loop):
    return Client([("localhost", 11211), ("localhost", 11212)])


@pytest.fixture(scope="session")
def key_generation():
    def _():
        cnt = 0
        base = time.time()
        while True:
            yield str(base + cnt).encode()
            cnt += 1

    return _()
