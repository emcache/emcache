import asyncio
import logging
import socket
from typing import Optional

logger = logging.getLogger(__name__)


EXISTS = b"EXISTS"
STORED = b"STORED"
TOUCHED = b"TOUCHED"
OK = b"OK"
DELETED = b"DELETED"
NOT_STORED = b"NOT_STORED"
NOT_FOUND = b"NOT_FOUND"


class BaseProtocol(asyncio.Protocol):
    _transport: Optional[asyncio.Transport]
    _loop: asyncio.AbstractEventLoop
    _closed: bool

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._transport = None
        self._closed = False

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
        raise NotImplementedError
