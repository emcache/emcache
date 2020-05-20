ctypedef unsigned int       uint32_t

cdef extern from "murmur3.h":

  void MurmurHash3_x64_128(const void *key, int len, uint32_t seed, void *out)
