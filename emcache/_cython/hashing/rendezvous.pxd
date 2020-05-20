cdef class RendezvousNode:
    cdef:
        bytes host_and_port
        readonly object node

        # Only used for break ties
        unsigned int hash_
