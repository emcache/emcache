import logging
from typing import List, Optional, Tuple, Union

from .._cython import cyemcache
from .base import BaseProtocol

logger = logging.getLogger(__name__)


class MemcacheAsciiProtocol(BaseProtocol):
    """Memcache ascii protocol communication.

    Ascii protocol communication uses a request/response pattern, all commands
    are translated to a specific request and waiting for a specific response.

    There is no concurrency within the same connection, and only one inflight
    command is expected. For providing concurrency Memcache prescribes the
    usage of different connections.
    """

    _parser: Optional[Union[cyemcache.AsciiOneLineParser, cyemcache.AsciiMultiLineParser]]

    def __init__(self) -> None:
        super().__init__()

        # Parser is configured during the execution of the command,
        # and will depend on the nature of the command
        self._parser = None

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
