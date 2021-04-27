cdef extern from "arpa/inet.h" nogil:

    int ntohl(int)
    int htonl(int)
