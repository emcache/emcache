cdef class BinaryMultiOpResponseParser:
    cdef:
        object future
        bytearray buffer_
        # attributes used by the parser globally
        unsigned int size 
        unsigned int parsed_bytes
        bint parsing_header
        bint last_op_reached 
        # attributes used by the parser for storing the
        # ongoing operation parsed
        unsigned int cur_status
        unsigned int cur_total_body_length
        unsigned int cur_extras_length
        unsigned int cur_key_length
        unsigned long long cur_cas
        # global store of all of the results as list
        list extras
        list keys
        list values 
        list cass 
