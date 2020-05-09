import asyncio
import time

from fastcache._cython import cyfastcache

NUM_ITERATIONS = 1_000_000
CR = 13  # \r
LF = 10  # \n


class OneLineParser:
    def __init__(self, response_event):
        self.response_event = response_event
        self.buffer_ = bytearray()
        self.pos = 0

    def feed_data(self, buffer_):
        self.buffer_.extend(buffer_)

        last = len(self.buffer_) - 1
        if last == 0:
            return

        if self.buffer_[last - 1] == CR and self.buffer_[last] == LF:
            self.response_event.set()


async def python_one_line_implementation():
    ev = asyncio.Event()
    start = time.time()
    for i in range(NUM_ITERATIONS):
        parser = OneLineParser(ev)
        parser.feed_data(b"asdfasdf\r")
        parser.feed_data(b"\n")
    elapsed = time.time() - start
    print("One line Python total time {}".format(elapsed))


async def cython_one_line_implementation():
    ev = asyncio.Event()
    start = time.time()
    for i in range(NUM_ITERATIONS):
        parser = cyfastcache.OneLineParser(ev)
        parser.feed_data(b"asdfasdf\r")
        parser.feed_data(b"\n")
    elapsed = time.time() - start
    print("One line Cython total time {}".format(elapsed))


async def cython_multi_line_implementation():
    ev = asyncio.Event()
    start = time.time()
    for i in range(NUM_ITERATIONS):
        parser = cyfastcache.MultiLineParser(ev)
        parser.feed_data(b"VALUE key 0 5\r\nvalue\r\n")
        parser.feed_data(b"END\r\n")
    elapsed = time.time() - start
    print("Multi line Cython total time {}".format(elapsed))


asyncio.run(python_one_line_implementation())
asyncio.run(cython_one_line_implementation())
asyncio.run(cython_multi_line_implementation())
