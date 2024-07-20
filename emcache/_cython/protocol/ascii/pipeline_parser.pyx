cimport cython
from libc.string cimport strcmp


cdef const char* MN = "MN\r\n"


@cython.freelist(32)
cdef class AsciiPipeLineParser:
    def __cinit__(self, object future):
        self.buffer_ = bytearray()

    def __init__(self, object future):
        self.future = future

    def feed_data(self, bytes buffer_):
        cdef char * c_buffer
        cdef int len_

        self.buffer_.extend(buffer_)

        # search last response to ending pipeline. `MN\r\n`
        len_ = len(self.buffer_)
        if len_ < 1:
            return

        c_buffer = self.buffer_
        if strcmp(<const char *> c_buffer + (len_ - 4), MN) == 0:
            self.future.set_result(None)

    def value(self):
        return self.buffer_[:-2]
