import asyncio
import time

import pytest

from fastcache import Client, NotStoredStorageCommandError

pytestmark = pytest.mark.asyncio


class TestCacheClient:
    async def test_timeout(self):
        unreachable_target = "0.0.0.1"
        client = Client(unreachable_target, 11211, timeout=0.1)
        with pytest.raises(asyncio.TimeoutError):
            await client.get(b"key")


@pytest.mark.parametrize(
    "flags,exptime,noreply",
    [
        (0, 0, False),
        (1, 0, False),
        # Expire immediately
        (0, -1, False),
        (0, 0, True),
    ],
)
class TestCacheClientStorageCommands:
    async def test_set(self, flags, exptime, noreply):
        client = Client("localhost", 11211)
        key_and_value = str(time.time()).encode()
        await client.set(key_and_value, key_and_value, flags=flags, exptime=exptime, noreply=noreply)

        value_retrieved = await client.get(key_and_value)

        if exptime == -1:
            assert value_retrieved is None
        else:
            assert key_and_value == value_retrieved

    async def test_add(self, flags, exptime, noreply):
        client = Client("localhost", 11211)
        key_and_value = str(time.time()).encode()
        await client.add(key_and_value, key_and_value)
        value_retrieved = await client.get(key_and_value)

        assert key_and_value == value_retrieved

        # adding a new value for the same key must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.add(key_and_value, key_and_value)

    async def test_replace(self, flags, exptime, noreply):
        client = Client("localhost", 11211)
        key_and_value = str(time.time()).encode()

        # replace a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.replace(key_and_value, key_and_value)

        # set the new key and replace the value.
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace")
        value_retrieved = await client.get(key_and_value)

        assert b"replace" == value_retrieved

    async def test_append(self, flags, exptime, noreply):
        client = Client("localhost", 11211)
        key_and_value = str(time.time()).encode()

        # append a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.append(key_and_value, key_and_value)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append")
        value_retrieved = await client.get(key_and_value)

        assert key_and_value + b"append" == value_retrieved

    async def test_prepend(self, flags, exptime, noreply):
        client = Client("localhost", 11211)
        key_and_value = str(time.time()).encode()

        # prepend a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.prepend(key_and_value, key_and_value)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend")
        value_retrieved = await client.get(key_and_value)

        assert b"prepend" + key_and_value == value_retrieved
