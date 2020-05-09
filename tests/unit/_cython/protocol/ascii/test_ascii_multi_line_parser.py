import pytest

from fastcache._cython import cyfastcache

pytestmark = pytest.mark.asyncio


class TestAsciiMultiLineParser:
    @pytest.mark.parametrize(
        "lines, keys, values",
        [
            # Different END arrivals
            ([b"END\r\n"], [], []),
            ([b"END\r", b"\n"], [], []),
            ([b"END", b"\r\n"], [], []),
            ([b"END", b"\r", b"\n"], [], []),
            # Different item and value arrivals
            ([b"VALUE key 0 5\r\nvalue\r\nEND\r\n"], [b"key"], [b"value"]),
            ([b"VALUE", b" key 0 5\r\nvalue\r\nEND\r\n"], [b"key"], [b"value"]),
            ([b"VALUE", b" key", b" 0 5\r\nvalue\r\nEND\r\n"], [b"key"], [b"value"]),
            ([b"VALUE", b" key", b" 0 5", b"\r\nvalue\r\nEND\r\n"], [b"key"], [b"value"]),
            ([b"VALUE", b" key", b" 0 5", b"\r\n", b"value\r\nEND\r\n"], [b"key"], [b"value"]),
            ([b"VALUE", b" key", b" 0 5", b"\r\n", b"value\r\n", b"END\r\n"], [b"key"], [b"value"]),
            # Multiple item and value arrivals
            (
                [b"VALUE key 0 5\r\nvalue\r\nVALUE key2 0 6\r\nvalue2\r\nEND\r\n"],
                [b"key", b"key2"],
                [b"value", b"value2"],
            ),
            (
                [b"VALUE key 0 5\r\nvalue\r\n", b"VALUE key2 0 6\r\nvalue2\r\nEND\r\n"],
                [b"key", b"key2"],
                [b"value", b"value2"],
            ),
        ],
    )
    async def test_feed_data_with_finish_event_set(self, event_loop, lines, keys, values):
        future = event_loop.create_future()
        parser = cyfastcache.AsciiMultiLineParser(future)
        for line in lines[:-1]:
            parser.feed_data(line)
            assert not future.done()

        parser.feed_data(lines[-1])
        assert future.done()
        assert parser.keys() == keys
        assert parser.values() == values
