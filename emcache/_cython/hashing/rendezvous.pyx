# MIT License
# Copyright (c) 2020-2024 Pau Freixes

cdef class RendezvousNode:
    """ Nodes will have a reference to a RendezvousNode object
    which will be used later by the `node_selection` function.

    The attribute `node` will look at the Python Node representation
    which will provide in some way connectivity to the selected node.
    """
    def __cinit__(self, str host, int port, object node):
        host_and_port = "{}{}".format(host, str(port)) 
        self.host_and_port = host_and_port.encode()
        self.hash_ = _hash(self.host_and_port)
        self.node = node


cdef unsigned int _hash(bytes data):
    """ Returns a 32 bit hash length as an integer.

    Internally the murmurhash function is used, using the 32 less significant bits.
    """
    cdef unsigned int result
    cdef char out[16]
    cdef char *c_data = data
    cdef unsigned int mask = 0xffffffff
    MurmurHash3_x64_128(c_data, len(data), 0, &out)
    result = (<char>out[3] & mask << 24) | (<char>out[2] & mask << 16) | (<char>out[1] & mask << 8) | <char>out[0] & mask
    return result


cpdef node_selection(bytes key, list rendezvous_nodes):
    """Find the node for a specific key.

    Follows the Rendezvou algorithm, with all of the nodes
    with the same weight. The node with more score will be the one
    selected.

    The list of nodes might change, due to nodes added ore removed,
    which would mean that some keys will be assigned to other nodes.

    The object returned should be the Python class representation of
    a node that would be enough for connecting to it.
    """
    cdef RendezvousNode rdz_node
    cdef RendezvousNode high_score_rdz_node
    cdef unsigned int high_score = 0
    cdef unsigned int score = 0
    cdef unsigned int hash_a
    cdef unsigned int hash_b

    if len(rendezvous_nodes) == 1:
        return rendezvous_nodes[0].node

    # calculate for the first node
    high_score_rdz_node = rendezvous_nodes[0]
    high_score = _hash(key + high_score_rdz_node.host_and_port)

    # check if other nodes could have a higher score
    for rdz_node in rendezvous_nodes[1:]:
        score = _hash(key + rdz_node.host_and_port)
        if score > high_score:
            high_score = score
            high_score_rdz_node = rdz_node
        elif score == high_score:
            if rdz_node.hash_ > high_score_rdz_node.hash_:
                high_score_rdz_node = rdz_node

    return high_score_rdz_node.node

def nodes_selection(object keys, list rendezvous_nodes) -> dict:
    """Find the nodes for a specific list of keys.

    For each key it uses the C version of `node_selection` function.
    """
    cdef object node
    cdef dict result

    result = {}

    if len(rendezvous_nodes) == 1:
        result[rendezvous_nodes[0].node] = list(keys)
        return result

    for key in keys:
        node = node_selection(key, rendezvous_nodes)
        if node in result:
            result[node].append(key)
        else:
            result[node] = [key]

    return result
