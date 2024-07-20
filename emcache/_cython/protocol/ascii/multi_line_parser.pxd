# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cdef class AsciiMultiLineParser:
    cdef object future
    cdef bytearray buffer_
    cdef list keys_
    cdef list values_
    cdef list flags_
    cdef list cas_
    cdef bytearray client_error_

    cdef void _parse(self, int len_)
