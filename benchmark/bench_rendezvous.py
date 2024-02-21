# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import time

import mmh3
from emcache._cython import cyemcache

NUM_ITERATIONS = 1_000_000
KEYS = [
    b"12345678",
    b"123456789101112123134",
    b"12345678123123123123123123123",
]


async def mmh3_hash32(num_hashes):
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            for _ in range(num_hashes):
                mmh3.hash(key)
    elapsed = time.time() - start
    print("mmh3_hash32 with {} hashes, total time {}".format(num_hashes, elapsed))


async def mmh3_hash64(num_hashes):
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            for _ in range(num_hashes):
                mmh3.hash(key)
    elapsed = time.time() - start
    print("mmh3_hash64 with {} hashes, total time {}".format(num_hashes, elapsed))


async def mmh3_hash128(num_hashes):
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            for _ in range(num_hashes):
                mmh3.hash(key)
    elapsed = time.time() - start
    print("mmh3_hash128 with {} hashes, total time {}".format(num_hashes, elapsed))


async def select_node(num_nodes):
    nodes = [object() for _ in range(num_nodes)]
    rendezvous_nodes = [cyemcache.RendezvousNode(str(i).encode(), node) for i, node in enumerate(nodes)]
    start = time.time()
    for i in range(NUM_ITERATIONS):
        for key in KEYS:
            cyemcache.node_selection(key, rendezvous_nodes)
    elapsed = time.time() - start
    print("Node Selection with {} nodes total time {}".format(num_nodes, elapsed))


asyncio.run(mmh3_hash64(1))
asyncio.run(mmh3_hash128(1))
asyncio.run(mmh3_hash32(1))
asyncio.run(mmh3_hash32(2))
asyncio.run(mmh3_hash32(4))
asyncio.run(mmh3_hash32(8))
asyncio.run(mmh3_hash32(16))
asyncio.run(select_node(1))
asyncio.run(select_node(2))
asyncio.run(select_node(4))
asyncio.run(select_node(8))
asyncio.run(select_node(16))
