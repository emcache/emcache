# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import logging
import re
import socket
from typing import Final, List, Optional, Tuple, Union

from ._address import MemcachedHostAddress, MemcachedUnixSocketPath
from ._cython import cyemcache
from .client_errors import AuthenticationError

try:
    import ssl as ssl_module
except ImportError:
    ssl_module = None

logger = logging.getLogger(__name__)


EXISTS = b"EXISTS"
STORED = b"STORED"
TOUCHED = b"TOUCHED"
OK = b"OK"
ERROR = b"ERROR"
DELETED = b"DELETED"
NOT_STORED = b"NOT_STORED"
NOT_FOUND = b"NOT_FOUND"
END = b"END"
VERSION = b"VERSION"


class AutoDiscoveryCommandParser:
    COMMAND_RE: Final = re.compile(rb"^CONFIG cluster 0 (\d+)\r\n")

    def __init__(self, future: asyncio.Future) -> None:
        self._future = future
        self._buffer = bytearray()
        self._autodiscovery: bool = False
        self._version: int = -1
        self._nodes: List[Tuple[str, str, int]] = []

    def feed_data(self, buffer: bytes) -> None:
        self._buffer.extend(buffer)

        if self._buffer.endswith(b"%s\r\n" % END):
            self._autodiscovery = True
            self.parse()
            self._future.set_result(None)
            return

        assert self._buffer.endswith(b"%s\r\n" % ERROR)
        self._autodiscovery = False
        self._future.set_result(None)

    def parse(self) -> None:
        try:
            if not (match := self.COMMAND_RE.search(self._buffer)):
                logger.error("Unable to find pattern %s in %r", self.COMMAND_RE.pattern, self._buffer)
                self._autodiscovery = False
                return

            output = self._buffer[match.end(0) : match.end(0) + int(match.group(1))].strip().split(b"\n")

            self._version = int(output[0])

            self._nodes = []
            for item in output[1].decode("utf-8").split():
                elements = item.split("|")
                self._nodes.append((elements[0], elements[1], int(elements[2])))
        except Exception:
            logger.exception("Unable to parse: %r", self._buffer)
            self._autodiscovery = False

    def autodiscovery(self) -> bool:
        return self._autodiscovery

    def version(self) -> int:
        return self._version

    def nodes(self) -> List[Tuple[str, str, int]]:
        return self._nodes

    def value(self):
        return self._buffer


class MemcacheAsciiProtocol(asyncio.Protocol):
    """Memcache ascii protocol communication.

    Ascii protocol communication uses a request/response pattern, all commands
    are translated to a specific request and waiting for a specific response.

    There is no concurrency within the same connection, and only one inflight
    command is expected. For providing concurrency Memcache prescribes the
    usage of different connections.
    """

    _parser: Optional[Union[cyemcache.AsciiOneLineParser, cyemcache.AsciiMultiLineParser, AutoDiscoveryCommandParser]]
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

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        sock = transport.get_extra_info("socket")
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        logger.debug("Connection made")

    def connection_lost(self, exc) -> None:
        if not self._closed:
            logger.warning(f"Connection lost: {exc}")

        self.close()

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self._transport.close()

    def eof_received(self) -> None:
        self.close()

    def closed(self) -> bool:
        return self._closed

    def data_received(self, data: bytes) -> None:
        if self._parser is None:
            raise RuntimeError(f"Receiving data when no parser is conifgured {data}")

        self._parser.feed_data(data)

    async def auth(self, username: str, password: str):
        result = await self.auth_command(username, password)
        if result == b"CLIENT_ERROR authentication failure":
            raise AuthenticationError(f"Fail authentication. Incorrect username or password. Return result {result}.")
        elif result != STORED:
            raise AuthenticationError(
                "Fail authentication. This server doesn't support SASL. "
                f"Not needed username and password. Return result {result}."
            )

    async def _extract_autodiscovery_data(self, data: bytes):
        try:
            future = self._loop.create_future()
            parser = AutoDiscoveryCommandParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            return parser.autodiscovery(), parser.version(), parser.nodes()
        finally:
            self._parser = None

    async def _extract_multi_line_data(self, data: bytes):
        try:
            future = self._loop.create_future()
            parser = cyemcache.AsciiMultiLineParser(future)
            self._parser = parser
            self._transport.write(data)
            await future
            keys, values, flags, cas, client_error = (
                parser.keys(),
                parser.values(),
                parser.flags(),
                parser.cas(),
                parser.client_error(),
            )
            return keys, values, flags, cas, client_error
        finally:
            self._parser = None

    async def _extract_one_line_data(self, data: bytes):
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

    async def fetch_command(
        self, cmd: bytes, keys: Tuple[bytes]
    ) -> Tuple[List[bytes], List[bytes], List[int], List[int], bytearray]:
        data = b"%b %b\r\n" % (cmd, b" ".join(keys))
        return await self._extract_multi_line_data(data)

    async def storage_command(
        self, command: bytes, key: bytes, value: bytes, flags: int, exptime: int, noreply: bool, cas: Optional[int]
    ) -> Optional[bytes]:
        if cas:
            extra = b" %a" % cas if not noreply else b" %a noreply" % cas
        else:
            extra = b"" if not noreply else b" noreply"

        data = b"%b %b %a %a %a%b\r\n%b\r\n" % (command, key, flags, exptime, len(value), extra, value)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def incr_decr_command(self, command: bytes, key: bytes, value: int, noreply: bool) -> Optional[bytes]:
        noreply = b" noreply" if noreply else b""
        data = b"%b %b %a%b\r\n" % (command, key, value, noreply)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def touch_command(self, key: bytes, exptime: int, noreply: bool) -> Optional[bytes]:
        noreply = b" noreply" if noreply else b""
        data = b"touch %b %a%b\r\n" % (key, exptime, noreply)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def flush_all_command(self, delay: int, noreply: bool) -> Optional[bytes]:
        noreply = b" noreply" if noreply else b""
        data = b"flush_all %a%b\r\n" % (delay, noreply)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def delete_command(self, key: bytes, noreply: bool) -> Optional[bytes]:
        noreply = b" noreply" if noreply else b""
        data = b"delete %b%b\r\n" % (key, noreply)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def autodiscovery(self) -> Tuple[bool, int, List[Tuple[str, str, int]]]:
        data = b"config get cluster\r\n"
        return await self._extract_autodiscovery_data(data)

    async def version_command(self) -> Optional[bytes]:
        data = b"version\r\n"
        return await self._extract_one_line_data(data)

    async def get_and_touch_command(
        self, cmd: bytes, exptime: int, keys: Tuple[bytes]
    ) -> Tuple[List[bytes], List[bytes], List[int], List[int], bytearray]:
        data = b"%b %a %b\r\n" % (cmd, exptime, b" ".join(keys))
        return await self._extract_multi_line_data(data)

    async def cache_memlimit_command(self, value: int, noreply: bool) -> Optional[bytes]:
        extra = b" noreply" if noreply else b""
        data = b"cache_memlimit %a%b\r\n" % (value, extra)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def stats_command(self, *args: str) -> Optional[bytes]:
        data = b"stats %b\r\n" % " ".join(args).encode() if args else b"stats\r\n"
        return await self._extract_one_line_data(data)

    async def verbosity_command(self, level: int, noreply: bool):
        extra = b" noreply" if noreply else b""
        data = b"verbosity %a%b\r\n" % (level, extra)

        if noreply:
            # fire and forget
            self._transport.write(data)
            return
        return await self._extract_one_line_data(data)

    async def auth_command(self, username: str, password: str) -> Optional[bytes]:
        value = f"{username} {password}".encode()
        data = b"set _ _ _ %a\r\n%b\r\n" % (len(value), value)
        return await self._extract_one_line_data(data)


async def create_protocol(
    address: Union[MemcachedHostAddress, MemcachedUnixSocketPath],
    ssl: bool,
    ssl_verify: bool,
    ssl_extra_ca: Optional[str],
    username: Optional[str],
    password: Optional[str],
    *,
    timeout: int = None,
) -> MemcacheAsciiProtocol:
    """Create a new connection which supports the Memcache protocol, if timeout is provided
    an `asyncio.TimeoutError` can be raised."""

    loop = asyncio.get_running_loop()

    if ssl:
        if not ssl_verify:
            ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS)
        elif ssl_verify and ssl_extra_ca:
            ssl = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS)
            ssl.verify_mode = ssl_module.CERT_REQUIRED
            ssl.load_default_certs()
            ssl.load_verify_locations(cafile=ssl_extra_ca)
        else:
            ssl = ssl_module.create_default_context()
    else:
        ssl = False

    if isinstance(address, MemcachedHostAddress):
        connect_coro = loop.create_connection(MemcacheAsciiProtocol, host=address.address, port=address.port, ssl=ssl)
    else:
        connect_coro = loop.create_unix_connection(MemcacheAsciiProtocol, path=address.path, ssl=ssl)
    if timeout is None:
        _, protocol = await connect_coro
    else:
        _, protocol = await asyncio.wait_for(connect_coro, timeout)

    # sasl auth via protocol
    if username and password:
        await protocol.auth(username, password)

    return protocol
