cimport cython
from libc.string cimport memcpy


@cython.freelist(32)
cdef class BinaryOneOpResponseParser:

    def __cinit__(self, object future):
        self.buffer_ = bytearray() 
        self.header_received = 0

    def __init__(self, future):
        self.future = future

    def start_parse(self):
        self.buffer_ = bytearray()

    def feed_data(self, bytes buffer_):
        cdef char* c_buffer
        cdef unsigned int network_value
        cdef unsigned int len_

        self.buffer_.extend(buffer_)

        len_ = len(self.buffer_)
        if self.header_received == 0 and len_ >= 24:
            self.header_received = 1

            c_buffer = self.buffer_

            # parse extras length (1 bytes)
            memcpy(<void *>&network_value, <const void *>c_buffer + 4, 1)
            self.extras_length = network_value & 0x000F

            # parse status (2 bytes)
            memcpy(<void *>&network_value, <const void *>c_buffer + 6, 2)
            self.status_ = ntohl(network_value << 16)

            # parse total body length (4 bytes)
            memcpy(<void *>&network_value, <const void *>c_buffer + 8, 4)
            self.total_body_length = ntohl(network_value)

            self.header_received = 1

        if len_ == self.total_body_length + 24:
            self.future.set_result(None)

    def status(self):
        return self.status_
