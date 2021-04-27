cdef class BinaryOneOpResponseParser:
    cdef:
        unsigned int status_
        unsigned int total_body_length
        unsigned int extras_length
        bint header_received
        object future
        bytearray buffer_
