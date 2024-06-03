# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cimport cython
from libc.string cimport strcmp


cdef const char* CRLF = "\r\n"


@cython.freelist(32)
cdef class AsciiOneLineParser:

    def __cinit__(self, object future):
        self.buffer_ = bytearray() 

    def __init__(self, future):
        self.future = future

    def start_parse(self):
        self.buffer_ = bytearray()

    def feed_data(self, bytes buffer_):
        cdef int len_

        self.buffer_.extend(buffer_)

        # search for the \r\n at the end of the final buffer,
        # otherwise more data needs to come

        len_ = len(self.buffer_)
        if len_ < 1:
            return

        cdef char* c_buffer = self.buffer_
        if strcmp(<const char *> c_buffer + (len_ - 2), CRLF) == 0:
            self.future.set_result(None)

    def value(self):
        return self.buffer_[:-2]
