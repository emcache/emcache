import time

import pytest

from fastcache import NotStoredStorageCommandError

pytestmark = pytest.mark.asyncio


class TestSet:
    async def test_set(self, client):
        key_and_value = str(time.time()).encode()
        await client.set(key_and_value, key_and_value)

        value_retrieved = await client.get(key_and_value)
        assert key_and_value == value_retrieved

    @pytest.mark.xfail
    async def test_set_flags(self, client):
        raise Exception("Implement when flags for get method is implemented")

    async def test_set_exptime(self, client):
        key_and_value = str(time.time()).encode()
        await client.set(key_and_value, key_and_value, exptime=-1)

        value_retrieved = await client.get(key_and_value)

        assert value_retrieved is None

    async def test_set_noreply(self, client):
        key_and_value = str(time.time()).encode()
        await client.set(key_and_value, key_and_value, noreply=True)

        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == key_and_value


class TestAdd:
    async def test_add(self, client):
        key_and_value = str(time.time()).encode()
        await client.add(key_and_value, key_and_value)
        value_retrieved = await client.get(key_and_value)
        assert key_and_value == value_retrieved

        # adding a new value for the same key must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.add(key_and_value, key_and_value)

    @pytest.mark.xfail
    async def test_add_flags(self, client):
        raise Exception("Implement when flags for get method is implemented")

    async def test_add_exptime(self, client):
        key_and_value = str(time.time()).encode()
        await client.add(key_and_value, key_and_value, exptime=-1)
        value_retrieved = await client.get(key_and_value)
        assert value_retrieved is None

        # add can be done again
        await client.add(key_and_value, key_and_value)

    async def test_add_noreply(self, client):
        key_and_value = str(time.time()).encode()
        await client.add(key_and_value, key_and_value, noreply=True)

        value_retrieved = await client.get(key_and_value)
        assert key_and_value == value_retrieved


class TestReplace:
    async def test_replace(self, client):
        key_and_value = str(time.time()).encode()

        # replace a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.replace(key_and_value, key_and_value)

        # set the new key and replace the value.
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace")
        value_retrieved = await client.get(key_and_value)

        assert b"replace" == value_retrieved

    @pytest.mark.xfail
    async def test_replace_flags(self, client):
        raise Exception("Implement when flags for get method is implemented")

    async def test_replace_exptime(self, client):
        key_and_value = str(time.time()).encode()

        # set the new key and replace the value with exptime = -1
        # for having it expired inmediatly
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", exptime=-1)
        value_retrieved = await client.get(key_and_value)
        assert value_retrieved is None

    async def test_replace_noreply(self, client):
        key_and_value = str(time.time()).encode()

        # set the new key and replace the value using noreply
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", noreply=True)
        value_retrieved = await client.get(key_and_value)

        assert b"replace" == value_retrieved


class TestAppend:
    async def test_append(self, client):
        key_and_value = str(time.time()).encode()

        # append a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.append(key_and_value, key_and_value)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append")
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == key_and_value + b"append"

    @pytest.mark.xfail
    async def test_append_flags(self, client):
        raise Exception("Implement when flags for get method is implemented")

    @pytest.mark.xfail
    async def test_append_exptime(self, client):
        key_and_value = str(time.time()).encode()

        # set the new key and append a value with exptime = -1
        # for having it expired inmediately
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append", exptime=-1)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved is None

    async def test_append_noreply(self, client):
        key_and_value = str(time.time()).encode()

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append", noreply=True)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == key_and_value + b"append"


class TestPrepend:
    async def test_prepend(self, client):
        key_and_value = str(time.time()).encode()

        # prepend a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.prepend(key_and_value, key_and_value)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend")
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == b"prepend" + key_and_value

    @pytest.mark.xfail
    async def test_prepend_flags(self, client):
        raise Exception("Implement when flags for get method is implemented")

    @pytest.mark.xfail
    async def test_prepend_exptime(self, client):
        key_and_value = str(time.time()).encode()

        # set the new key and prepend a value with exptime = -1
        # for having it epxired inmediately
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend", exptime=-1)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved is None

    async def test_prepend_noreply(self, client):
        key_and_value = str(time.time()).encode()

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend", noreply=True)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == b"prepend" + key_and_value
