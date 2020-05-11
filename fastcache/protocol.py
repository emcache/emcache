import asyncio
import logging
import socket
from typing import List, Optional, Union

from ._cython import cyfastcache

logger = logging.getLogger(__name__)


class MemcacheAsciiProtocol(asyncio.Protocol):
    """Memcache ascii protocol communication.

    Ascii protocol communication uses a request/response pattern, all commands
    are translated to a specific request and waiting for a specific response.

    There is no concurrency within the same connection, and only one inflight
    command is expected. For providing concurrency Memcache prescribes the
    usage of different connections.
    """

    _parser: Optional[Union[cyfastcache.AsciiOneLineParser, cyfastcache.AsciiMultiLineParser]]
    _transport: Optional[asyncio.Transport]
    _loop: asyncio.AbstractEventLoop
    _closed: bool

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._transport = None
        self._closed = False

        # Parser is configured during the execution of the command,
        # and will depend on the nature of the command
        self._parser = None

    def connection_made(self, transport) -> None:
        self._transport = transport
        sock = transport.get_extra_info("socket")
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        logger.debug("Connection made")

    def connection_lost(self, exc) -> None:
        if self._closed:
            return

        logger.warning(f"Connection lost: {exc}")

    def data_received(self, data: bytes) -> None:
        self._parser.feed_data(data)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._transport.close()

    async def get_cmd(self, key: bytes) -> List[bytes]:
        data = b"get " + key + b"\r\n"
        future = self._loop.create_future()
        parser = cyfastcache.AsciiMultiLineParser(future)
        self._parser = parser
        self._transport.write(data)
        await future
        return parser.keys(), parser.values()

    async def set_cmd(self, key: bytes, value: bytes) -> bytes:
        len_value = str(len(value)).encode()
        data = b"set " + key + b" 0 0 " + len_value + b"\r\n"
        data += value
        data += b"\r\n"
        future = self._loop.create_future()
        parser = cyfastcache.AsciiOneLineParser(future)
        self._parser = parser
        self._transport.write(data)
        await future
        return parser.value()


async def create_protocol(host: str, port: int) -> MemcacheAsciiProtocol:
    loop = asyncio.get_running_loop()
    _, protocol = await loop.create_connection(MemcacheAsciiProtocol, host=host, port=port)
    return protocol
