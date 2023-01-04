from libc.stdio cimport printf

cdef int MAX_KEY_LENGTH = 250

def is_key_valid(bytes key) -> bool:
    """Validates if the key is an acceptable Memcache key.

    From Memcache documentation, the key must not include control characters
    or whitespace, and must not take more than 250 characters.
    """
    cdef int c

    if len(key) > MAX_KEY_LENGTH:
        return False

    ustring = key.decode("UTF-8")

    for uchar in ustring:
        c = ord(uchar)
        if c <= b' ' or c == 0x7f:
            return False

    return True
