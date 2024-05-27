# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import time

import pytest

pytestmark = pytest.mark.asyncio


NUM_MANY_KEYS = 100


class TestGet:
    async def test_get(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        item = await client.get(key_and_value)
        assert item.value == key_and_value
        assert item.flags is None
        assert item.cas is None

    async def test_get_return_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        item = await client.get(key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.flags == 1
        assert item.cas is None

    async def test_get_return_flags_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        item = await client.get(key_and_value, return_flags=True)
        assert item is None


class TestGets:
    async def test_gets(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        item = await client.gets(key_and_value)
        assert item.value == key_and_value
        assert item.flags is None
        assert item.cas is not None

    async def test_gets_return_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        item = await client.gets(key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.cas is not None
        assert item.flags == 1

    async def test_gets_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        item = await client.gets(key_and_value)
        assert item is None


class TestGetMany:
    async def test_get_many(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        items = await client.get_many(keys_and_values)

        # Check that all keys are retrieved and they have the right values
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].flags is None, keys_and_values))
        assert all(map(lambda k: items[k].cas is None, keys_and_values))

    async def test_get_return_flags(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value, flags=1)

        items = await client.get_many(keys_and_values, return_flags=True)

        # Check that all keys are retrieved and they have the right values and flags
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].flags == 1, keys_and_values))
        assert all(map(lambda k: items[k].cas is None, keys_and_values))

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

        items = await client.get_many(keys)

        # Check that all keys are retrieved and they have the right values, not
        # found keys must not appear in the result
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))


class TestGetsMany:
    async def test_gets_many(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        items = await client.gets_many(keys_and_values)

        # Check that all keys are retrieved and they have the right values and the
        # cas value is not None
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].cas is not None, keys_and_values))
        assert all(map(lambda k: items[k].flags is None, keys_and_values))

    async def test_gets_return_flags(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value, flags=1)

        items = await client.gets_many(keys_and_values, return_flags=True)

        # Check that all keys are retrieved and they have the right values and flags
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].cas is not None, keys_and_values))
        assert all(map(lambda k: items[k].flags == 1, keys_and_values))

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

        items = await client.gets_many(keys)

        # Check that all keys are retrieved and they have the right values, not
        # found keys must not appear in the result
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))


class TestGat:
    async def test_gat(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        item = await client.gat(0, key_and_value)
        assert item.value == key_and_value
        assert item.flags is None
        assert item.cas is None

    async def test_gat_return_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        item = await client.gat(0, key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.flags == 1
        assert item.cas is None

    async def test_gat_return_flags_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        item = await client.gat(0, key_and_value, return_flags=True)
        assert item is None


class TestGats:
    async def test_gats(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value)

        item = await client.gats(0, key_and_value)
        assert item.value == key_and_value
        assert item.flags is None
        assert item.cas is not None

    async def test_gats_return_flags(self, client, key_generation):
        key_and_value = next(key_generation)
        await client.set(key_and_value, key_and_value, flags=1)

        item = await client.gats(0, key_and_value, return_flags=True)
        assert item.value == key_and_value
        assert item.cas is not None
        assert item.flags == 1

    async def test_gats_key_not_found(self, client, key_generation):
        key_and_value = next(key_generation)

        item = await client.gats(0, key_and_value)
        assert item is None


class TestGatMany:
    async def test_gat_many(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        items = await client.gat_many(0, keys_and_values)

        # Check that all keys are retrieved and they have the right values
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].flags is None, keys_and_values))
        assert all(map(lambda k: items[k].cas is None, keys_and_values))

    async def test_gat_return_flags(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value, flags=1)

        items = await client.gat_many(0, keys_and_values, return_flags=True)

        # Check that all keys are retrieved and they have the right values and flags
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].flags == 1, keys_and_values))
        assert all(map(lambda k: items[k].cas is None, keys_and_values))

    async def test_gat_many_with_not_found_keys(self, client, key_generation):
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

        items = await client.gat_many(0, keys)

        # Check that all keys are retrieved and they have the right values, not
        # found keys must not appear in the result
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))


class TestGatsMany:
    async def test_gats_many(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value)

        items = await client.gats_many(0, keys_and_values)

        # Check that all keys are retrieved and they have the right values and the
        # cas value is not None
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].cas is not None, keys_and_values))
        assert all(map(lambda k: items[k].flags is None, keys_and_values))

    async def test_gats_return_flags(self, client, key_generation):
        keys_and_values = [next(key_generation) for _ in range(NUM_MANY_KEYS)]

        for key_and_value in keys_and_values:
            await client.set(key_and_value, key_and_value, flags=1)

        items = await client.gats_many(0, keys_and_values, return_flags=True)

        # Check that all keys are retrieved and they have the right values and flags
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
        assert all(map(lambda k: items[k].cas is not None, keys_and_values))
        assert all(map(lambda k: items[k].flags == 1, keys_and_values))

    async def test_gats_many_with_not_found_keys(self, client, key_generation):
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

        items = await client.gats_many(0, keys)

        # Check that all keys are retrieved and they have the right values, not
        # found keys must not appear in the result
        assert all(map(lambda k: k in items, keys_and_values))
        assert all(map(lambda k: items[k].value == k, keys_and_values))
