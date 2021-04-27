cimport cython
from libc.string cimport memcpy


def binary_build_request_storage(bytes command, bytes key, bytes value, int flags, int exptime, long long cas):
    cdef unsigned int len_key = len(key)
    cdef unsigned int len_value = len(value)
    cdef char* c_buffer
    cdef char* c_key
    cdef char* c_value
    cdef unsigned int len_body_n = htonl(len_key + len_value + 8)
    cdef unsigned int len_key_n = htonl(len_key)
    cdef unsigned int len_value_n = htonl(len_value)
    cdef unsigned int exptime_network_value = htonl(exptime)
    cdef unsigned char op

    if command == b"set":
        op = HEADER_OP_SET 
    elif command == b"cas":
        # Unfortunatelly binary protocol does not implement the CAS command
        # as is implemented by the ascii protocol, it basically uses the SET
        # (or other storage comnands) and the CAS bytes expected in the header
        # request.
        op = HEADER_OP_SET 
    elif command == b"add":
        op = HEADER_OP_ADD
    elif command == b"replace":
        op = HEADER_OP_REPLACE

    buffer_ = bytearray(24 + len_key + len_value + 8)

    # Cast to C native types
    c_buffer = buffer_
    c_key = key
    c_value = value

    # header
    memcpy(<void *>c_buffer, &HEADER_MAGIC_NUMBER, 1)
    # set/add/replace
    memcpy(<void *>c_buffer + 1, &op, 1)
    # len key
    memcpy(<void *>c_buffer + 2, <void *>&len_key_n+2, 2)
    # len extras + not used
    memcpy(<void *>c_buffer + 4, &HEADER_EXTRA_LEN, 4)
    # len body
    memcpy(<void *>c_buffer + 8, &len_body_n, 4)
    # opaque
    memcpy(<void *>c_buffer + 12, &HEADER_ZEROS, 4)
    # cas (can we consider it an opaque 8 bytes value that does not need htonl?)
    memcpy(<void *>c_buffer + 16, &cas, 8)
    # body
    ## flags (can we consider it an opaque 4 bytes value that does not need htonl?)
    memcpy(<void *>c_buffer + 24, &flags, 4)
    memcpy(<void *>c_buffer + 28, &exptime_network_value, 4)
    memcpy(<void *>c_buffer + 32, c_key, len_key)
    memcpy(<void *>c_buffer + 32 + len_key, c_value, len_value)

    return buffer_

def binary_build_request_get(list keys):
    cdef int idx
    cdef unsigned int total_keys = len(keys)
    cdef bytes key
    cdef unsigned int total_keys_len = 0
    cdef unsigned int len_key
    cdef char* c_buffer
    cdef char* c_key
    cdef void* offset
    cdef unsigned int len_key_n = 0
    cdef unsigned int len_body_n = 0

    # Allocate space for all of the keys
    for idx in range(total_keys):
        total_keys_len += len(keys[idx])
  
    buffer_ = bytearray((24 * total_keys) + total_keys_len)
    c_buffer = buffer_

    offset = <void *>c_buffer
    # send N-1 operations using getq which holds
    # any response from memcache until a none silent
    # operation is sent (reserved for the last one).
    # Moreover, GETQK does not return misses.
    for idx in range(total_keys - 1):
        key = keys[idx] 

        # Cast to C native types
        len_key = len(key)
        len_key_n = htonl(len_key)
        len_body_n = len_key_n
        c_key = key

        # header
        memcpy(offset, &HEADER_MAGIC_NUMBER, 1)
        # getkq
        memcpy(offset + 1, &HEADER_OP_GETKQ, 1)
        # len key
        memcpy(offset + 2, <void *>&len_key_n+2, 2)
        # len extras + not used
        memcpy(offset + 12, &HEADER_ZEROS, 4)
        # len body
        memcpy(offset + 8, &len_body_n, 4)
        # opaque
        memcpy(offset + 12, &HEADER_ZEROS, 4)
        # cas (TODO)
        memcpy(offset + 16, &HEADER_ZEROS, 4)
        memcpy(offset + 20, &HEADER_ZEROS, 4)
        # body
        memcpy(offset + 24, c_key, len_key)

        # move the offset ahead
        offset = <void *>offset + 24 + len_key

    # Last operation needs to be a GETK, and will be used
    # for knowing when uncork the response from the server
    # and for the parser to known which is the last message
    # to be parsed.
    key = keys[total_keys - 1] 

    # Cast to C native types
    len_key = len(key)
    len_key_n = htonl(len_key)
    len_body_n = len_key_n
    c_key = key

    # header
    memcpy(offset, &HEADER_MAGIC_NUMBER, 1)
    # get
    memcpy(offset + 1, &HEADER_OP_GETK, 1)
    # len key
    memcpy(offset + 2, <void *>&len_key_n+2, 2)
    # len extras + not used
    memcpy(offset + 12, &HEADER_ZEROS, 4)
    # len body
    memcpy(offset + 8, &len_body_n, 4)
    # opaque
    memcpy(offset + 12, &HEADER_ZEROS, 4)
    # cas (TODO)
    memcpy(offset + 16, &HEADER_ZEROS, 4)
    memcpy(offset + 20, &HEADER_ZEROS, 4)
    # body
    memcpy(offset + 24, c_key, len_key)

    return buffer_
