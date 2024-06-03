# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cdef class RendezvousNode:
    cdef:
        bytes rendezvous_id
        readonly object node

        # Only used for break ties
        unsigned int hash_
