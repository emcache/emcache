# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import os

import pytest

from emcache import create_client

pytestmark = pytest.mark.asyncio


EXTRA_CA = os.path.join(os.path.dirname(__file__), "data", "rootCA.crt")
EXTRA_CA_INVALID = os.path.join(os.path.dirname(__file__), "data", "rootCA_invalid.crt")


class TestSSL:
    async def test_no_ssl_fails(self, memcached_address_3):
        client = await create_client([memcached_address_3], timeout=1.0, ssl=False)
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")
        await client.close()

    async def test_ssl_no_verify(self, memcached_address_3):
        client = await create_client([memcached_address_3], timeout=1.0, ssl=True, ssl_verify=False)
        await client.get(b"key")
        await client.close()

    async def test_ssl_verify_no_extra_ca(self, memcached_address_3):
        client = await create_client([memcached_address_3], timeout=1.0, ssl=True, ssl_verify=True)
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")
        await client.close()

    async def test_ssl_verify_extra_ca(self, memcached_address_3):
        client = await create_client(
            [memcached_address_3], timeout=1.0, ssl=True, ssl_verify=True, ssl_extra_ca=EXTRA_CA
        )
        await client.get(b"key")
        await client.close()

    async def test_ssl_verify_extra_ca_invalid(self, memcached_address_3):
        client = await create_client(
            [memcached_address_3], timeout=1.0, ssl=True, ssl_verify=True, ssl_extra_ca=EXTRA_CA_INVALID
        )
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")
        await client.close()
