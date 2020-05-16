import pytest

from fastcache import Client


@pytest.fixture
async def client(event_loop):
    return Client("localhost", 11211)
