import pytest

from emcache._cython import cyemcache


class TestIsKeyValid:
    @pytest.mark.parametrize(
        "key,encoding",
        [
            (b"foo", "ascii"),
            (b"bar", "ascii"),
            (b"foobar", "ascii"),
            (b"foobarfoo", "ascii"),
            (b"123foobarfoo123", "ascii"),
            ("ñ".encode("utf8"), "utf8"),
            ("ñ".encode("utf16"), "utf16"),
        ],
    )
    def test_valid_keys(self, key, encoding):
        assert cyemcache.is_key_valid(key, encoding) is True

    @pytest.mark.parametrize("key", [b"foo", "ñ".encode("utf8")])
    def test_default_encoding(self, key):
        """Test that key validation is successful when not explicitly passing
        encoding for both ascii and utf8.
        """
        assert cyemcache.is_key_valid(key) is True

    def test_default_encoding_failure(self):
        """Test that key validation encounters an error for encodings that aren't subsets of utf8."""

        with pytest.raises(UnicodeDecodeError):
            cyemcache.is_key_valid("ñ".encode("utf16"))

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
