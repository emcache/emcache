import pytest

from emcache._cython import cyemcache


class TestIsKeyValid:
    @pytest.mark.parametrize("key", [b"foo", b"bar", b"foobar", b"foobarfoo", b"123foobarfoo123", "ñ".encode("utf8")])
    def test_valid_keys(self, key):
        assert cyemcache.is_key_valid(key) is True

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
        assert cyemcache.is_key_valid(key) is False
