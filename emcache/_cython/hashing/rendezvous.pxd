# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cdef class RendezvousNode:
    cdef bytes rendezvous_id
    cdef readonly object node
    # Only used for break ties
    cdef unsigned int hash_
