# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import re
import time

from emcache._cython import cyemcache

NUM_ITERATIONS = 1_000_000
KEYS = [
    # typical 8 bytes KEYS?
    # A valid one
    b"12345678",
    # An valid one
    b"12345678\n",
]


_valid_key_re = re.compile(b"^[\x21-\x7e]{1,250}$")


async def aiomemcache_implementation():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            _valid_key_re.match(key)
    elapsed = time.time() - start
    print("Vesion 1 Cython total time {}".format(elapsed))


async def cython_version_1_implementation():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            cyemcache.is_key_valid_version_1(key)
    elapsed = time.time() - start
    print("Vesion 1 Cython total time {}".format(elapsed))


async def cython_version_2_implementation():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            cyemcache.is_key_valid_version_2(key)
    elapsed = time.time() - start
    print("Vesion 2 Cython total time {}".format(elapsed))


asyncio.run(aiomemcache_implementation())
asyncio.run(cython_version_1_implementation())
asyncio.run(cython_version_2_implementation())
