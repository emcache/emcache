cimport cython
from libc.string cimport strncmp


cdef const char* END = "END\r\n"


@cython.freelist(32)
cdef class AsciiMultiLineParser:

    def __cinit__(self, object future):
        self.buffer_ = bytearray()
        self.values_ = []
        self.keys_ = []
        self.flags_ = []

    def __init__(self, future):
        self.future = future

    def start_parse(self):
        self.buffer_ = bytearray()

    def feed_data(self, bytes buffer_):
        cdef int len_
        cdef char* c_buffer

        self.buffer_.extend(buffer_)

        len_ = len(self.buffer_)
        if len_ < 1:
            return
        
        c_buffer = self.buffer_
        if len_ >= 5:
            if strcmp(<const char *> c_buffer + (len_ - 5), END) == 0:
                self._parse(len_)
                self.future.set_result(None)
                return

    cdef void _parse(self, int len_):
        cdef bytes item
        cdef bytes value
        cdef int start_line_pos = 0
        cdef int current_pos = 0
        cdef int value_size = 0
        cdef char* c_buffer = self.buffer_

        # iterate until the END
        while current_pos < (len_ - 5):

            if strncmp(<const char *> c_buffer + current_pos, CRLF, 2) != 0:
                current_pos += 1
                continue

            # End of line found, item found

            item = <bytes> c_buffer[start_line_pos:current_pos]
            _, key, flags, size = item.split(b" ")
            value_size = int(size)
            self.keys_.append(key)
            self.flags_.append(int(flags))

            # skip the CRLF
            current_pos += 2

            # parse the value
            value = <bytes> c_buffer[current_pos:current_pos+value_size]
            self.values_.append(value)
            current_pos += value_size

            # skip the CRLF
            current_pos += 2

            # save the place where the next line starts
            start_line_pos = current_pos

    def keys(self):
        return self.keys_

    def values(self):
        return self.values_

    def flags(self):
        return self.flags_
