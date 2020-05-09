import asyncio
import time

import mmh3

from fastcache._cython import cyfastcache

NUM_ITERATIONS = 1_000_000


async def python_serializer():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        data = b"get " + b"fooooooo" + b"\r\n"
    elapsed = time.time() - start
    print("python serializer, total time {}".format(elapsed))


async def cython_serializer():
    start = time.time()
    concat3 = cyfastcache.concat3
    for i in range(NUM_ITERATIONS):
        data = concat3(b"get ", b"fooooooo", b"\r\n")
    elapsed = time.time() - start
    print("cython serializer, total time {}".format(elapsed))


asyncio.run(python_serializer())
asyncio.run(cython_serializer())
