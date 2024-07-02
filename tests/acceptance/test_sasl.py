# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import logging
import os
import sys
from pathlib import Path

import pytest

from emcache import MemcachedHostAddress, create_client
from emcache.client_errors import AuthenticationError

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.DEBUG)

with open(Path(os.path.dirname(__file__)) / "data" / "auth_pwd.txt", "r") as f:
    username, password = f.readline().rstrip().split(":")


@pytest.fixture
async def authed_client(event_loop):
    client = await create_client(
        [MemcachedHostAddress("localhost", 11214)], timeout=1.0, username=username, password=password
    )
    try:
        yield client
    finally:
        await client.close()


class TestAuth:
    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth(self, authed_client):
        await authed_client.get(b"key")

    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth_error_no_userpass(self):
        client = await create_client([MemcachedHostAddress("localhost", 11214)], timeout=1.0)
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")

    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth_error_no_password(self):
        with pytest.raises(ValueError):
            await create_client([MemcachedHostAddress("localhost", 11214)], username=username, timeout=1.0)

    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth_error_no_username(self):
        with pytest.raises(ValueError):
            await create_client([MemcachedHostAddress("localhost", 11214)], password=password, timeout=1.0)

    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth_error_wrong_userpass(self):
        client = await create_client(
            [MemcachedHostAddress("localhost", 11214)], username="wrong", password="wrong", timeout=1.0
        )
        with pytest.raises(AuthenticationError):
            await client.get(b"key")

    @pytest.mark.skipif(sys.platform == "darwin", reason="This server is not built with SASL support.")
    async def test_auth_error_disable_sasl(self):
        client = await create_client(
            [MemcachedHostAddress("localhost", 11211)], username="wrong", password="wrong", timeout=1.0
        )
        with pytest.raises(AuthenticationError):
            await client.get(b"key")
