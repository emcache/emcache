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
