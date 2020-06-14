import asyncio
import logging
import socket
from typing import List, Optional, Tuple, Union

from ._cython import cyemcache

logger = logging.getLogger(__name__)


EXISTS = b"EXISTS"
STORED = b"STORED"
TOUCHED = b"TOUCHED"
OK = b"OK"
DELETED = b"DELETED"
NOT_STORED = b"NOT_STORED"
NOT_FOUND = b"NOT_FOUND"


class MemcacheAsciiProtocol(asyncio.Protocol):
    """Memcache ascii protocol communication.

    Ascii protocol communication uses a request/response pattern, all commands
    are translated to a specific request and waiting for a specific response.

    There is no concurrency within the same connection, and only one inflight
    command is expected. For providing concurrency Memcache prescribes the
    usage of different connections.
    """

    _parser: Optional[Union[cyemcache.AsciiOneLineParser, cyemcache.AsciiMultiLineParser]]
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
        logger.warning(f"Connection lost: {exc}")
        self.close()

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._transport.close()

    def eof_received(self):
        self.close()

    def closed(self) -> bool:
        return self._closed

    def data_received(self, data: bytes) -> None:
        if self._parser is None:
            raise RuntimeError(f"Receiving data when no parser is conifgured {data}")

        self._parser.feed_data(data)

    async def fetch_command(
        self, cmd: bytes, keys: Tuple[bytes]
    ) -> Tuple[List[bytes], List[bytes], List[int], List[int]]:
        try:
            data = cmd + b" " + b" ".join(keys) + b"\r\n"
            future = self._loop.create_future()
            parser = cyemcache.AsciiMultiLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            keys, values, flags, cas = parser.keys(), parser.values(), parser.flags(), parser.cas()
            return keys, values, flags, cas
        finally:
            self._parser = None

    async def storage_command(
        self, command: bytes, key: bytes, value: bytes, flags: int, exptime: int, noreply: bool, cas: Optional[int]
    ) -> Optional[bytes]:

        exptime_value = str(exptime).encode()
        flags_value = str(flags).encode()
        len_value = str(len(value)).encode()

        if cas:
            cas_value = str(cas).encode()
            extra = b" " + cas_value if not noreply else b" " + cas_value + b" noreply"
        else:
            extra = b"" if not noreply else b" noreply"

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
            + extra
            + b"\r\n"
            + value
            + b"\r\n"
        )

        if noreply:
            # fire and forget
            self._transport.write(data)
            return None

        try:
            future = self._loop.create_future()
            parser = cyemcache.AsciiOneLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            result = parser.value()
            return result
        finally:
            self._parser = None

    async def incr_decr_command(self, command: bytes, key: bytes, value: int, noreply: bool) -> Optional[bytes]:
        if noreply:
            noreply = b" noreply"
        else:
            noreply = b""

        data = command + b" " + key + b" " + str(value).encode() + noreply + b"\r\n"

        if noreply:
            # fire and forget
            self._transport.write(data)
            return None

        try:
            future = self._loop.create_future()
            parser = cyemcache.AsciiOneLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            result = parser.value()
            return result
        finally:
            self._parser = None

    async def touch_command(self, key: bytes, exptime: int, noreply: bool) -> Optional[bytes]:
        if noreply:
            noreply = b" noreply"
        else:
            noreply = b""

        data = b"touch " + key + b" " + str(exptime).encode() + noreply + b"\r\n"

        if noreply:
            # fire and forget
            self._transport.write(data)
            return None

        try:
            future = self._loop.create_future()
            parser = cyemcache.AsciiOneLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            result = parser.value()
            return result
        finally:
            self._parser = None

    async def flush_all_command(self, delay: int, noreply: bool) -> Optional[bytes]:
        if noreply:
            noreply = b" noreply"
        else:
            noreply = b""

        data = b"flush_all " + str(delay).encode() + noreply + b"\r\n"

        if noreply:
            # fire and forget
            self._transport.write(data)
            return None

        try:
            future = self._loop.create_future()
            parser = cyemcache.AsciiOneLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            result = parser.value()
            return result
        finally:
            self._parser = None

    async def delete_command(self, key: bytes, noreply: bool) -> Optional[bytes]:
        if noreply:
            noreply = b" noreply"
        else:
            noreply = b""

        data = b"delete " + key + noreply + b"\r\n"

        if noreply:
            # fire and forget
            self._transport.write(data)
            return None

        try:
            future = self._loop.create_future()
            parser = cyemcache.AsciiOneLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            result = parser.value()
            return result
        finally:
            self._parser = None


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
