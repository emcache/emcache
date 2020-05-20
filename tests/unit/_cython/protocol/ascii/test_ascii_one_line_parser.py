import pytest

from emcache._cython import cyemcache

pytestmark = pytest.mark.asyncio


class TestAsciiOneLineParser:
    @pytest.mark.parametrize(
        "lines, value",
        [([b"\r\n"], b""), ([b"\r", b"\n"], b""), ([b"foo", b"\r\n"], b"foo"), ([b"foo", b"\r", b"\n"], b"foo")],
    )
    async def test_feed_data_with_finish_event_set(self, event_loop, lines, value):
        future = event_loop.create_future()
        parser = cyemcache.AsciiOneLineParser(future)
        for line in lines[:-1]:
            parser.feed_data(line)
            assert not future.done()

        parser.feed_data(lines[-1])
        assert future.done()
        assert parser.value() == value
