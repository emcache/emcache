from libc.stdio cimport printf

cdef int MAX_KEY_LENGTH = 250

def is_key_valid(bytes key, encoding="utf8") -> bool:
    """Validates if the key is an acceptable Memcache key.

    From Memcache documentation, the key must not include control characters
    or whitespace, and must not take more than 250 characters.
    """
    cdef int c

    if len(key) > MAX_KEY_LENGTH:
        return False

    decoded = key.decode(encoding)

    for d_char in decoded:
        c = ord(d_char)
        if c <= b' ' or c == 0x7f:
            return False

    return True
