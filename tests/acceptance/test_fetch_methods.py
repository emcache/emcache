import time

import pytest

pytestmark = pytest.mark.asyncio


NUM_MANY_KEYS = 100


class TestGet:
    async def test_get(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        value_retrieved = await client.get(key_and_value)
        assert value_retrieved == key_and_value

    async def test_get_return_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        value_retrieved, flags_returned = await client.get(key_and_value, return_flags=True)
        assert value_retrieved == key_and_value
        assert flags_returned == 1

    async def test_get_return_flags_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        value_retrieved, flags_returned = await client.get(key_and_value, return_flags=True)
        assert value_retrieved is None
        assert flags_returned is None


class TestGets:
    async def test_gets(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        value_retrieved, cas_retrieved = await client.gets(key_and_value)
        assert value_retrieved == key_and_value
        assert cas_retrieved is not None

    async def test_gets_return_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        value_retrieved, cas_retrieved, flags_retrieved = await client.gets(key_and_value, return_flags=True)
        assert value_retrieved == key_and_value
        assert cas_retrieved is not None
        assert flags_retrieved == 1

    async def test_gets_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        value_retrieved, cas_retrieved = await client.gets(key_and_value)
        assert value_retrieved is None
        assert cas_retrieved is None

    async def test_gets_return_flags_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        value_retrieved, cas_retrieved, flags_retrieved = await client.gets(key_and_value, return_flags=True)
        assert value_retrieved is None
        assert cas_retrieved is None
        assert flags_retrieved is None


class TestGetMany:
    async def test_get_many(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        keys_and_values_retrieved = await client.get_many(keys_and_values)

        # Check that all keys are retrieved and they have the right values
        assert all(map(lambda k: k in keys_and_values_retrieved, keys_and_values))
        assert all(map(lambda k: keys_and_values_retrieved[k] == k, keys_and_values))

    async def test_get_return_flags(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value, flags=1)

        keys_and_values_retrieved = await client.get_many(keys_and_values, return_flags=True)

        # Check that all keys are retrieved and they have the right values and flags
        assert all(map(lambda k: k in keys_and_values_retrieved, keys_and_values))
        assert all(map(lambda k: keys_and_values_retrieved[k][0] == k, keys_and_values))
        assert all(map(lambda k: keys_and_values_retrieved[k][1] == 1, keys_and_values))

    async def test_get_many_with_not_found_keys(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        # we add two keys that should not be present, one at the beginning, one
        # at the middle and one at the end of the list
        not_found_key1 = str(time.time()) + "not_found_1"
        not_found_key2 = str(time.time()) + "not_found_2"
        not_found_key3 = str(time.time()) + "not_found_3"
        keys = (
            [not_found_key1.encode()]
            + keys_and_values[: int(len(keys_and_values) / 2)]
            + [not_found_key2.encode()]
            + keys_and_values[int(len(keys_and_values) / 2) :]  # noqa: E203
            + [not_found_key3.encode()]
        )

        keys_and_values_retrieved = await client.get_many(keys)

        # Check that all keys are retrieved and they have the right values, not
        # found keys should basically no appear int the result
        assert all(map(lambda k: k in keys_and_values_retrieved, keys_and_values))
        assert all(map(lambda k: keys_and_values_retrieved[k] == k, keys_and_values))


class TestGetsMany:
    async def test_gets_many(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        keys_and_values_and_cas_retrieved = await client.gets_many(keys_and_values)

        # Check that all keys are retrieved and they have the right values and the
        # cas value is not None
        assert all(map(lambda k: k in keys_and_values_and_cas_retrieved, keys_and_values))
        assert all(map(lambda k: keys_and_values_and_cas_retrieved[k][0] == k, keys_and_values))
        assert all(map(lambda k: keys_and_values_and_cas_retrieved[k][1] is not None, keys_and_values))

    async def test_gets_return_flags(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value, flags=1)

        keys_and_values_and_cas_retrieved = await client.gets_many(keys_and_values, return_flags=True)

        # Check that all keys are retrieved and they have the right values and flags
        assert all(map(lambda k: k in keys_and_values_and_cas_retrieved, keys_and_values))
        assert all(map(lambda k: keys_and_values_and_cas_retrieved[k][0] == k, keys_and_values))
        assert all(map(lambda k: keys_and_values_and_cas_retrieved[k][1] is not None, keys_and_values))
        assert all(map(lambda k: keys_and_values_and_cas_retrieved[k][2] == 1, keys_and_values))

    async def test_gets_many_with_not_found_keys(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        # we add two keys that should not be present, one at the beginning, one
        # at the middle and one at the end of the list
        not_found_key1 = str(time.time()) + "not_found_1"
        not_found_key2 = str(time.time()) + "not_found_2"
        not_found_key3 = str(time.time()) + "not_found_3"
        keys = (
            [not_found_key1.encode()]
            + keys_and_values[: int(len(keys_and_values) / 2)]
            + [not_found_key2.encode()]
            + keys_and_values[int(len(keys_and_values) / 2) :]  # noqa: E203
            + [not_found_key3.encode()]
        )

        keys_and_values_and_cas_retrieved = await client.gets_many(keys)

        # Check that all keys are retrieved and they have the right values, not
        # found keys should basically no appear int the result
        assert all(map(lambda k: k in keys_and_values_and_cas_retrieved, keys_and_values))
        assert all(map(lambda k: keys_and_values_and_cas_retrieved[k][0] == k, keys_and_values))
