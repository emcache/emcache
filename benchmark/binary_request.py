import asyncio
import time
from struct import pack

from emcache._cython import cyemcache

NUM_ITERATIONS = 4_000_000
KEY = b"foo"


def _ascii_encoding(keys):
    return b"get" + b" " + b" ".join(keys) + b"\r\n"


async def ascii_version():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        _ascii_encoding([KEY] * 32)
    elapsed = time.time() - start
    print("Ascii encoding total time {}".format(elapsed))


async def binary_version():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        cyemcache.binary_build_request_get([KEY] * 32)
    elapsed = time.time() - start
    print("Cython total time {}".format(elapsed))


async def struct_pack_version():
    start = time.time()
    for i in range(NUM_ITERATIONS):
        pack(
            f"!BBHBBHIIQ{len(KEY)}s",
            # Header
            # Magic number
            0x80,
            # Get
            0x00,
            # Len key
            len(KEY),
            # Extras length
            0,
            # Not used
            0,
            0,
            # Total body length
            len(KEY),
            # Opaque
            0,
            # Cas
            0,
            # End Header
            # Body
            KEY
            # End Body
        )
    elapsed = time.time() - start
    print("Struct Pack Version total time {}".format(elapsed))


asyncio.run(binary_version())
asyncio.run(ascii_version())
