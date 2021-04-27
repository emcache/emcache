cimport cython
from libc.string cimport memcpy


@cython.freelist(32)
cdef class BinaryMultiOpResponseParser:

    def __cinit__(self, object future):
        self.buffer_ = bytearray() 

        self.parsed_bytes = 0
        self.size = 0
        self.parsing_header = 1
        self.last_op_reached = 0

        self.cur_total_body_length = 0
        self.cur_status = 0
        self.cur_extras_length = 0
        self.cur_key_length = 0
        self.cur_cas = 0

        self.extras = []
        self.keys = []
        self.values = []
        self.cass = []

    def __init__(self, future):
        self.future = future

    def start_parse(self):
        self.buffer_ = bytearray()

    def feed_data(self, bytes buffer_):
        cdef char* c_buffer
        cdef unsigned int network_value
        cdef unsigned int len_ = len(buffer_)
        cdef unsigned int offset
        cdef unsigned int body_size
        cdef unsigned int flags = 0

        self.buffer_.extend(buffer_)
        self.size += len_

        while True:
            if self.parsing_header == 1 and self.size - self.parsed_bytes >= 24:
                # skip the bytes that have been already parsed
                c_buffer = self.buffer_
                c_buffer = c_buffer + self.parsed_bytes

                # parse opcode (1 bytes) and check if its the last one
                memcpy(<void *>&network_value, <const void *>c_buffer + 1, 1)
                opcode = network_value & 0x000F
                if opcode == HEADER_OP_GETK:
                    self.last_op_reached = 1
                # parse key length (2 bytes)
                memcpy(<void *>&network_value, <const void *>c_buffer + 2, 2)
                self.cur_key_length = ntohl(network_value << 16)
                # parse extras length (1 bytes)
                memcpy(<void *>&network_value, <const void *>c_buffer + 4, 1)
                self.cur_extras_length = network_value & 0x000F
                # parse status (2 bytes)
                memcpy(<void *>&network_value, <const void *>c_buffer + 6, 2)
                self.cur_status = ntohl(network_value << 16)
                # parse total body length (4 bytes)
                memcpy(<void *>&network_value, <const void *>c_buffer + 8, 4)
                self.cur_total_body_length = ntohl(network_value)
                # skip 4 bytes opaque
                # parse CAS (8 bytes)
                memcpy(<void *>&self.cur_cas, <const void *>c_buffer + 16, 8)

                # mark for start parsing the content
                self.parsed_bytes += 24
                self.parsing_header = 0

            elif self.parsing_header == 0 and self.size - self.parsed_bytes >= self.cur_total_body_length:
                if self.cur_status == RESPONSE_STATUS_NO_ERROR:
                    offset = self.parsed_bytes
                    c_buffer = self.buffer_
                    memcpy(<void *>&flags, <const void *>c_buffer + offset, self.cur_extras_length)
                    self.extras.append(flags)

                    offset = self.parsed_bytes + self.cur_extras_length
                    self.keys.append(bytes(self.buffer_[offset:offset+self.cur_key_length]))
                    offset = self.parsed_bytes + self.cur_extras_length + self.cur_key_length
                    body_size = self.cur_total_body_length - self.cur_extras_length - self.cur_key_length
                    self.values.append(bytes(self.buffer_[offset:offset+body_size]))
                    self.cass.append(self.cur_cas)
                elif self.cur_status == RESPONSE_STATUS_NOT_FOUND:
                    pass
                else:
                    raise NotImplemented()

                # check if we parsed the content from the last operation expected
                if self.last_op_reached: 
                    self.future.set_result(None)
                    break
                else:
                    # mark for start parsing another header
                    self.parsed_bytes += self.cur_total_body_length
                    self.parsing_header = 1
            else:
                break

    def value(self):
        return self.keys, self.values, self.extras, self.cass
