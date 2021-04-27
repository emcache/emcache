import asyncio
import logging
from enum import Enum
from typing import Optional

from .ascii_ import MemcacheAsciiProtocol
from .binary import MemcacheBinaryProtocol

try:
    import ssl as ssl_module
except ImportError:
    ssl_module = None

logger = logging.getLogger(__name__)


class Protocol(Enum):
    BINARY = "binary"
    ASCII = "ascii"


async def create_protocol(
    host: str,
    port: int,
    protocol: Protocol,
    ssl: bool,
    ssl_verify: bool,
    ssl_extra_ca: Optional[str],
    *,
    timeout: int = None
) -> MemcacheAsciiProtocol:
    """ Create a new connection which supports the Memcache protocol, if timeout is provided
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

    protocolClass = MemcacheBinaryProtocol if protocol == Protocol.BINARY else MemcacheAsciiProtocol

    if timeout is None:
        _, protocol = await loop.create_connection(protocolClass, host=host, port=port, ssl=ssl)
    else:
        _, protocol = await asyncio.wait_for(
            loop.create_connection(protocolClass, host=host, port=port, ssl=ssl), timeout
        )
    return protocol
