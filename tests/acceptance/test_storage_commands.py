import asyncio
import sys

import pytest

from emcache import NotStoredStorageCommandError

pytestmark = pytest.mark.asyncio


class TestSet:
    async def test_set(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        item = await client.get(key_and_value)
        assert item.value == key_and_value

    async def test_set_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        item = await client.get(key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.flags == 1

    async def test_set_exptime(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, exptime=1)
        await asyncio.sleep(2)
        item = await client.get(key_and_value)

        assert item is None

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_set_noreply(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, noreply=True)

        item = await client.get(key_and_value)
        assert item.value == key_and_value


class TestAdd:
    async def test_add(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value)
        item = await client.get(key_and_value)
        assert item.value == key_and_value

        # adding a new value for the same key must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.add(key_and_value, key_and_value)

    async def test_add_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value, flags=1)
        item = await client.get(key_and_value, return_flags=True)

        assert item.value == key_and_value
        assert item.flags == 1

    async def test_add_exptime(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value, exptime=1)
        await asyncio.sleep(2)
        item = await client.get(key_and_value)
        assert item is None

        # add can be done again and not raises any exception
        await client.add(key_and_value, key_and_value)

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_add_noreply(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value, noreply=True)

        item = await client.get(key_and_value)
        assert item.value == key_and_value


class TestReplace:
    async def test_replace(self, client, key_generation):
        key_and_value = next(key_generation)

        # replace a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.replace(key_and_value, key_and_value)

        # set the new key and replace the value.
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace")
        item = await client.get(key_and_value)

        assert item.value == b"replace"

    async def test_replace_flags(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and replace the value.
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", flags=1)
        item = await client.get(key_and_value, return_flags=True)

        assert item.value == b"replace"
        assert item.flags == 1

    async def test_replace_exptime(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and replace the value with exptime = -1
        # for having it expired inmediatly
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", exptime=1)
        await asyncio.sleep(2)
        item = await client.get(key_and_value)
        assert item is None

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_replace_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and replace the value using noreply
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", noreply=True)
        item = await client.get(key_and_value)

        assert item.value == b"replace"


class TestAppend:
    async def test_append(self, client, key_generation):
        key_and_value = next(key_generation)

        # append a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.append(key_and_value, key_and_value)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append")
        item = await client.get(key_and_value)

        assert item.value == key_and_value + b"append"

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_append_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append", noreply=True)
        item = await client.get(key_and_value)

        assert item.value == key_and_value + b"append"


class TestPrepend:
    async def test_prepend(self, client, key_generation):
        key_and_value = next(key_generation)

        # prepend a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.prepend(key_and_value, key_and_value)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend")
        item = await client.get(key_and_value)

        assert item.value == b"prepend" + key_and_value

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_prepend_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend", noreply=True)
        item = await client.get(key_and_value)

        assert item.value == b"prepend" + key_and_value


class TestCas:
    async def test_cas(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        item = await client.gets(key_and_value)

        # Swap using the right cas token
        await client.cas(key_and_value, b"new_value", item.cas)

        # Check
        new_item = await client.get(key_and_value)
        assert new_item.value == b"new_value"

    async def test_cas_invalid_cas_token(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        item = await client.gets(key_and_value)

        # Swap using a wrong token cas token
        wrong_cas_token = item.cas + 1
        with pytest.raises(NotStoredStorageCommandError):
            await client.cas(key_and_value, b"new_value", wrong_cas_token)

    async def test_cas_flags(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        item = await client.gets(key_and_value)

        # Swap using the right cas token and upating the flags
        await client.cas(key_and_value, b"new_value", item.cas, flags=1)

        # Check
        new_item = await client.get(key_and_value, return_flags=True)
        assert new_item.value == b"new_value"
        assert new_item.flags == 1

    async def test_cas_exptime(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        item = await client.gets(key_and_value)

        # Swap using the right cas token and updating the exptime
        await client.cas(key_and_value, b"new_value", item.cas, exptime=1)
        await asyncio.sleep(2)

        # Check that got expired since we gave an exptime value of 1
        item = await client.get(key_and_value, return_flags=True)
        assert item is None

    @pytest.mark.skipif(sys.platform == "darwin", reason="https://github.com/memcached/memcached/issues/681")
    async def test_cas_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        item = await client.gets(key_and_value)

        # Swap using the right cas token and no reply
        await client.cas(key_and_value, b"new_value", item.cas, noreply=True)

        # Check
        item = await client.get(key_and_value)
        assert item.value == b"new_value"
