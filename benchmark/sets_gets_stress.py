import argparse
import asyncio
import logging
import random
import time

import uvloop

from fastcache import Client

uvloop.install()


MAX_NUMBER_OF_KEYS = 65536


async def cmd_set(key: bin, client: Client) -> None:
    await client.set(key, b"Some value")


async def cmd_get(key: bin, client: Client) -> None:
    await client.get(key)


async def benchmark(desc: str, coro_op, max_keys: int, client: Client, concurrency: int, duration: int) -> None:
    print("Starting benchmark {}".format(desc))

    not_finish_benchmark = True

    async def incr():
        nonlocal not_finish_benchmark
        times = []
        while not_finish_benchmark:
            key = random.randint(0, max_keys)
            start = time.monotonic()
            await coro_op(str(key).encode(), client)
            elapsed = time.monotonic() - start
            times.append(elapsed)
        return times

    tasks = [asyncio.ensure_future(incr()) for _ in range(concurrency)]

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
        "--memcache-address", help="Redis address, by default redis://localhost", default="127.0.0.1",
    )
    parser.add_argument(
        "--memcache-port", help="Memcache port, by default 11211", default=11211,
    )
    parser.add_argument(
        "--concurrency", help="Number of concurrency clients, by default 32", type=int, default=32,
    )
    parser.add_argument(
        "--duration", help="Test duration in seconds, by default 60", type=int, default=60,
    )
    args = parser.parse_args()

    client = Client("localhost", 11211)

    await benchmark("SET", cmd_set, MAX_NUMBER_OF_KEYS, client, args.concurrency, args.duration)
    await benchmark("GET", cmd_set, MAX_NUMBER_OF_KEYS, client, args.concurrency, args.duration)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
