# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cdef class AsciiMultiLineParser:
    cdef:
        object future
        bytearray buffer_
        list keys_
        list values_
        list flags_
        list cas_
        bytearray client_error_

    cdef void _parse(self, int len_)
