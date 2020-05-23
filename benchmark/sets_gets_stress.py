import argparse
import asyncio
import logging
import random
import time
from typing import List

import uvloop

from emcache import Client, create_client

uvloop.install()


MAX_NUMBER_OF_KEYS = 65536


async def cmd_set(key: bytes, client: Client) -> None:
    await client.set(key, b"Some value")


async def cmd_get(key: bytes, client: Client) -> None:
    await client.get(key)


async def cmd_get_many(keys: List[bytes], client: Client) -> None:
    await client.get_many(keys)


async def benchmark(desc: str, coro_op, max_keys: int, client: Client, concurrency: int, duration: int) -> None:
    print("Starting benchmark {}".format(desc))

    not_finish_benchmark = True

    async def run():
        nonlocal not_finish_benchmark
        times = []
        while not_finish_benchmark:
            key_base = random.randint(0, max_keys)
            if desc == "GET_MANY":
                keys = [str(key_base + i).encode() for i in range(4)]
            else:
                keys = str(key_base).encode()
            start = time.monotonic()
            await coro_op(keys, client)
            elapsed = time.monotonic() - start
            times.append(elapsed)
        return times

    tasks = [asyncio.ensure_future(run()) for _ in range(concurrency)]

    await asyncio.sleep(duration)

    not_finish_benchmark = False
    while not all([task.done() for task in tasks]):
        await asyncio.sleep(0)

    times = []
    for task in tasks:
        times += task.result()

    times.sort()

    total_ops = len(times)
    avg = sum(times) / total_ops

    p90 = times[int((90 * total_ops) / 100)]
    p99 = times[int((99 * total_ops) / 100)]

    print("Tests results:")
    print("\tOps/sec: {0}".format(int(total_ops / duration)))
    print("\tAvg: {0:.6f}".format(avg))
    print("\tP90: {0:.6f}".format(p90))
    print("\tP99: {0:.6f}".format(p99))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--memcache-address",
        help="Memcache address, by default 127.0.0.1. Multiple addresses can be provided using a coma seaparator",
        default="127.0.0.1",
    )
    parser.add_argument(
        "--memcache-port",
        help="Memcache port, by default 11211. Multiple addresses can be provided using a coma seaparator",
        default="11211",
    )
    parser.add_argument(
        "--concurrency", help="Number of concurrency clients, by default 32", type=int, default=32,
    )
    parser.add_argument(
        "--duration", help="Test duration in seconds, by default 60", type=int, default=60,
    )
    parser.add_argument(
        "--test",
        help="Test to be executed set_get or set_get_many, by default sets and gets stress",
        type=str,
        default="set_get",
    )
    args = parser.parse_args()

    if args.memcache_address:
        addresses = args.memcache_address.split(",")

    if args.memcache_port:
        ports = args.memcache_port.split(",")

    hosts = [(host, int(port)) for host, port in zip(addresses, ports)]

    client = await create_client(hosts, timeout=None, max_connections=args.concurrency)

    if args.test == "set_get":
        await benchmark("SET", cmd_set, MAX_NUMBER_OF_KEYS, client, args.concurrency, args.duration)
        await benchmark("GET", cmd_get, MAX_NUMBER_OF_KEYS, client, args.concurrency, args.duration)
    elif args.test == "set_get_many":
        await benchmark("SET", cmd_set, MAX_NUMBER_OF_KEYS, client, args.concurrency, args.duration)
        await benchmark("GET_MANY", cmd_get_many, MAX_NUMBER_OF_KEYS, client, args.concurrency, args.duration)
    else:
        raise ValueError("Unkown test")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
