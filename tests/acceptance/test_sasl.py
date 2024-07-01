# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import logging
import os
import sys
from pathlib import Path

import pytest

from emcache import MemcachedHostAddress, create_client
from emcache.client_errors import AuthenticationError, AuthenticationNotSupportedError

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.DEBUG)

with open(Path(os.path.dirname(__file__)) / "data" / "auth_pwd.txt", "r") as f:
    username, password = f.readline().rstrip().split(":")


@pytest.fixture()
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
    async def test_auth_with_errors(self):
        with pytest.raises(asyncio.TimeoutError):
            client_no_userpass = await create_client([MemcachedHostAddress("localhost", 11214)], timeout=1.0)
            await client_no_userpass.get(b"key")
        with pytest.raises(AuthenticationError):
            await create_client([MemcachedHostAddress("localhost", 11214)], username=username, timeout=1.0)
        with pytest.raises(AuthenticationError):
            await create_client([MemcachedHostAddress("localhost", 11214)], password=password, timeout=1.0)
        with pytest.raises(AuthenticationError):
            client_wrong_userpass = await create_client(
                [MemcachedHostAddress("localhost", 11214)], username="wrong", password="wrong", timeout=1.0
            )
            await client_wrong_userpass.get(b"key")
        with pytest.raises(AuthenticationNotSupportedError):
            client_no_need_sasl_but_used = await create_client(
                [MemcachedHostAddress("localhost", 11211)], username="wrong", password="wrong", timeout=1.0
            )
            await client_no_need_sasl_but_used.get(b"key")
