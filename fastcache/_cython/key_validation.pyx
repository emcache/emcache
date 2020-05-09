cdef int MAX_KEY_LENGTH = 250

def is_key_valid(bytes key) -> bool:
    """Validates if the key is an acceptable Memcache key.
    
    From Memcache documentation, the key must not include control characters
    or whitespace, and must not take more than 250 characters.
    """
    cdef int c

    if len(key) > MAX_KEY_LENGTH:
        return False

    for c in key:
        if c < 33 or c > 126:
            return False

    return True
