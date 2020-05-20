import pytest

from emcache import NotStoredStorageCommandError

pytestmark = pytest.mark.asyncio


class TestSet:
    async def test_set(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == key_and_value

    async def test_set_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        value_retrieved, flags_returned = await client.get(key_and_value, return_flags=True)
        assert value_retrieved == key_and_value
        assert flags_returned == 1

    async def test_set_exptime(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, exptime=-1)

        value_retrieved = await client.get(key_and_value)

        assert value_retrieved is None

    async def test_set_noreply(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, noreply=True)

        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == key_and_value


class TestAdd:
    async def test_add(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value)
        value_retrieved = await client.get(key_and_value)
        assert key_and_value == value_retrieved

        # adding a new value for the same key must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.add(key_and_value, key_and_value)

    async def test_add_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value, flags=1)
        value_retrieved, flags_returned = await client.get(key_and_value, return_flags=True)

        assert value_retrieved == key_and_value
        assert flags_returned == 1

    async def test_add_exptime(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value, exptime=-1)
        value_retrieved = await client.get(key_and_value)
        assert value_retrieved is None

        # add can be done again
        await client.add(key_and_value, key_and_value)

    async def test_add_noreply(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.add(key_and_value, key_and_value, noreply=True)

        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == key_and_value


class TestReplace:
    async def test_replace(self, client, key_generation):
        key_and_value = next(key_generation)

        # replace a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.replace(key_and_value, key_and_value)

        # set the new key and replace the value.
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace")
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == b"replace"

    async def test_replace_flags(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and replace the value.
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", flags=1)
        value_retrieved, flags_retrieved = await client.get(key_and_value, return_flags=True)

        assert value_retrieved == b"replace"
        assert flags_retrieved == 1

    async def test_replace_exptime(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and replace the value with exptime = -1
        # for having it expired inmediatly
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", exptime=-1)
        value_retrieved = await client.get(key_and_value)
        assert value_retrieved is None

    async def test_replace_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and replace the value using noreply
        await client.set(key_and_value, key_and_value)
        await client.replace(key_and_value, b"replace", noreply=True)
        value_retrieved = await client.get(key_and_value)

        assert b"replace" == value_retrieved


class TestAppend:
    async def test_append(self, client, key_generation):
        key_and_value = next(key_generation)

        # append a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.append(key_and_value, key_and_value)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append")
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == key_and_value + b"append"

    @pytest.mark.xfail
    async def test_append_flags(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append", flags=1)
        value_retrieved, flags_retrieved = await client.get(key_and_value, return_flags=True)

        assert value_retrieved == key_and_value + b"append"
        assert flags_retrieved == 1

    @pytest.mark.xfail
    async def test_append_exptime(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and append a value with exptime = -1
        # for having it expired inmediately
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append", exptime=-1)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved is None

    async def test_append_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and append a value.
        await client.set(key_and_value, key_and_value)
        await client.append(key_and_value, b"append", noreply=True)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == key_and_value + b"append"


class TestPrepend:
    async def test_prepend(self, client, key_generation):
        key_and_value = next(key_generation)

        # prepend a value for a key that does not exist must fail
        with pytest.raises(NotStoredStorageCommandError):
            await client.prepend(key_and_value, key_and_value)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend")
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == b"prepend" + key_and_value

    @pytest.mark.xfail
    async def test_prepend_flags(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend", flags=1)
        value_retrieved, flags_retrieved = await client.get(key_and_value, return_flags=True)

        assert value_retrieved == b"prepend" + key_and_value
        assert flags_retrieved == 1

    @pytest.mark.xfail
    async def test_prepend_exptime(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and prepend a value with exptime = -1
        # for having it epxired inmediately
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend", exptime=-1)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved is None

    async def test_prepend_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # set the new key and prepend a value.
        await client.set(key_and_value, key_and_value)
        await client.prepend(key_and_value, b"prepend", noreply=True)
        value_retrieved = await client.get(key_and_value)

        assert value_retrieved == b"prepend" + key_and_value


class TestCas:
    async def test_cas(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        _, cas_retrieved = await client.gets(key_and_value)

        # Swap using the right cas token
        await client.cas(key_and_value, b"new_value", cas_retrieved)

        # Check
        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == b"new_value"

    async def test_cas_invalid_cas_token(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        _, cas_retrieved = await client.gets(key_and_value)

        # Swap using a wrong token cas token
        wrong_cas_token = cas_retrieved + 1
        with pytest.raises(NotStoredStorageCommandError):
            await client.cas(key_and_value, b"new_value", wrong_cas_token)

    async def test_cas_flags(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        _, cas_retrieved = await client.gets(key_and_value)

        # Swap using the right cas token and upating the flags
        await client.cas(key_and_value, b"new_value", cas_retrieved, flags=1)

        # Check
        value_retrieved, flags_retrieved = await client.get(key_and_value, return_flags=True)
        assert value_retrieved == b"new_value"
        assert flags_retrieved == 1

    async def test_cas_exptime(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        _, cas_retrieved = await client.gets(key_and_value)

        # Swap using the right cas token and updating the exptime
        await client.cas(key_and_value, b"new_value", cas_retrieved, exptime=-1)

        # Check that got expired since we gave an exptime value of -1
        value_retrieved, flags_retrieved = await client.get(key_and_value, return_flags=True)
        assert value_retrieved is None
        assert flags_retrieved is None

    async def test_cas_noreply(self, client, key_generation):
        key_and_value = next(key_generation)

        # Store a new value and retrieve the cas value assigned
        await client.set(key_and_value, key_and_value)
        _, cas_retrieved = await client.gets(key_and_value)

        # Swap using the right cas token and no reply
        await client.cas(key_and_value, b"new_value", cas_retrieved, noreply=True)

        # Check
        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == b"new_value"
