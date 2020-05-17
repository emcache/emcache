import pytest

pytestmark = pytest.mark.asyncio


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
