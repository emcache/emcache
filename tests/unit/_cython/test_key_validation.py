import pytest

from fastcache._cython import cyfastcache


class TestIsKeyValid:
    @pytest.mark.parametrize("key", [b"foo", b"bar", b"foobar", b"foobarfoo", b"123foobarfoo123"])
    def test_valid_keys(self, key):
        assert cyfastcache.is_key_valid(key) is True

    @pytest.mark.parametrize(
        "key",
        [
            # whitespaces are not allowed
            b" ",
            b"foo ",
            # control chars are not allowed
            b"foo\n",
            b"foo\r",
            # max length is 250
            b" " * 251,
        ],
    )
    def test_invalid_keys(self, key):
        assert cyfastcache.is_key_valid(key) is False
