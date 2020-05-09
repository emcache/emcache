cdef class AsciiMultiLineParser:
    cdef:
        object future
        bytearray buffer_
        list keys_
        list values_

    cdef void _parse(self, int len_)
