import asyncio
import logging
import socket
from typing import List, Optional, Tuple, Union

from ._cython import cyfastcache

logger = logging.getLogger(__name__)


STORED = b"STORED"
NOT_STORED = b"NOT_STORED"


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
        if self._parser is None:
            raise RuntimeError(f"Receiving data when no parser is conifgured {data}")

        self._parser.feed_data(data)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._transport.close()

    async def get_cmd(self, key: bytes) -> Tuple[List[bytes], List[bytes], List[int]]:
        data = b"get " + key + b"\r\n"
        future = self._loop.create_future()
        parser = cyfastcache.AsciiMultiLineParser(future)
        self._parser = parser
        self._transport.write(data)
        await future
        keys, values, flags = parser.keys(), parser.values(), parser.flags()
        self._parser = None
        return keys, values, flags

    async def storage_command(
        self, command: bytes, key: bytes, value: bytes, flags: int, exptime: int, noreply: bool
    ) -> Optional[bytes]:

        exptime_value = str(exptime).encode()
        flags_value = str(flags).encode()
        len_value = str(len(value)).encode()
        noreply_value = b"" if not noreply else b" noreply"

        data = (
            command
            + b" "
            + key
            + b" "
            + flags_value
            + b" "
            + exptime_value
            + b" "
            + len_value
            + noreply_value
            + b"\r\n"
            + value
            + b"\r\n"
        )

        if noreply:
            # fire and forget
            self._transport.write(data)
            return None
        else:
            future = self._loop.create_future()
            parser = cyfastcache.AsciiOneLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            value = parser.value()
            self._parser = None
            return value


async def create_protocol(host: str, port: int, *, timeout: int = None) -> MemcacheAsciiProtocol:
    """ Create a new connection which supports the Memcache protocol, if timeout is provided
    an `asyncio.TimeoutError` can be raised."""
    loop = asyncio.get_running_loop()
    if timeout is None:
        _, protocol = await loop.create_connection(MemcacheAsciiProtocol, host=host, port=port)
    else:
        _, protocol = await asyncio.wait_for(
            loop.create_connection(MemcacheAsciiProtocol, host=host, port=port), timeout
        )
    return protocol
