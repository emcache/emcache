# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cdef int MAX_KEY_LENGTH = 250

cpdef int is_key_valid(unsigned char* key):
    """Validates if the key is an acceptable Memcache key.
    
    From Memcache documentation, the key must not include control characters
    or whitespace, and must not take more than 250 characters.
    """
    cdef unsigned char c
    cdef int len_key
    cdef int index

    len_key = len(key)

    if len_key > MAX_KEY_LENGTH:
        return 0
    for index in range(len_key):
        c = key[index]
        if c < 33 or c == 127:
            return 0
    return 1
