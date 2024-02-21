# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import pytest

from emcache._cython import cyemcache

pytestmark = pytest.mark.asyncio


class TestAsciiMultiLineParser:
    @pytest.mark.parametrize(
        "lines, keys, flags, values, cas",
        [
            # Different END arrivals
            ([b"END\r\n"], [], [], [], []),
            ([b"END\r", b"\n"], [], [], [], []),
            ([b"END", b"\r\n"], [], [], [], []),
            ([b"END", b"\r", b"\n"], [], [], [], []),
            # Different item and value arrivals
            ([b"VALUE key 0 5\r\nvalue\r\nEND\r\n"], [b"key"], [0], [b"value"], [None]),
            ([b"VALUE", b" key 0 5\r\nvalue\r\nEND\r\n"], [b"key"], [0], [b"value"], [None]),
            ([b"VALUE", b" key", b" 0 5\r\nvalue\r\nEND\r\n"], [b"key"], [0], [b"value"], [None]),
            ([b"VALUE", b" key", b" 0 5", b"\r\nvalue\r\nEND\r\n"], [b"key"], [0], [b"value"], [None]),
            ([b"VALUE", b" key", b" 0 5", b"\r\n", b"value\r\nEND\r\n"], [b"key"], [0], [b"value"], [None]),
            ([b"VALUE", b" key", b" 0 5", b"\r\n", b"value\r\n", b"END\r\n"], [b"key"], [0], [b"value"], [None]),
            # Multiple item and value arrivals
            (
                [b"VALUE key 0 5\r\nvalue\r\nVALUE key2 0 6\r\nvalue2\r\nEND\r\n"],
                [b"key", b"key2"],
                [0, 0],
                [b"value", b"value2"],
                [None, None],
            ),
            (
                [b"VALUE key 0 5\r\nvalue\r\n", b"VALUE key2 0 6\r\nvalue2\r\nEND\r\n"],
                [b"key", b"key2"],
                [0, 0],
                [b"value", b"value2"],
                [None, None],
            ),
            # Multiple item and value arrivals with cas
            (
                [b"VALUE key 0 5 1\r\nvalue\r\nVALUE key2 0 6 2\r\nvalue2\r\nEND\r\n"],
                [b"key", b"key2"],
                [0, 0],
                [b"value", b"value2"],
                [1, 2],
            ),
        ],
    )
    async def test_feed_data_with_finish_event_set(self, event_loop, lines, keys, flags, values, cas):
        future = event_loop.create_future()
        parser = cyemcache.AsciiMultiLineParser(future)
        for line in lines[:-1]:
            parser.feed_data(line)
            assert not future.done()

        parser.feed_data(lines[-1])
        assert future.done()
        assert parser.keys() == keys
        assert parser.values() == values
        assert parser.flags() == flags
        assert parser.cas() == cas
