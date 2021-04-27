import logging
from struct import pack
from typing import List, Optional, Tuple, Union

from .._cython import cyemcache
from .base import NOT_STORED, STORED, BaseProtocol

logger = logging.getLogger(__name__)


class MemcacheBinaryProtocol(BaseProtocol):
    """Memcache binary protocol communication.

    https://github.com/memcached/old-wiki/blob/master/MemcacheBinaryProtocol.wiki
    """

    _parser: Optional[Union[cyemcache.BinaryMultiOpResponseParser, cyemcache.BinaryOneOpResponseParser]]

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
            req = cyemcache.binary_build_request_get(list(keys))
            future = self._loop.create_future()
            parser = cyemcache.BinaryMultiOpResponseParser(future)
            self._parser = parser
            self._transport.write(req)
            await future
            keys, values, extras, cas = parser.value()
            return keys, values, extras, cas
        finally:
            self._parser = None

    async def storage_command(
        self, command: bytes, key: bytes, value: bytes, flags: int, exptime: int, noreply: bool, cas: Optional[int]
    ) -> Optional[bytes]:

        req = cyemcache.binary_build_request_storage(command, key, value, flags, exptime, cas or 0)

        try:
            future = self._loop.create_future()
            parser = cyemcache.BinaryOneOpResponseParser(future)
            self._parser = parser
            self._transport.write(req)
            await future
            status = parser.status()
            if status == 0:
                return STORED
            elif status == 1:
                # when replace command is used, the error returned is a key not found
                # if the key does not exist, this diverge from the ascii protocol that
                # returns a NOT_STORED. We try to make the everything compatible to the
                # ascii version
                return NOT_STORED
            elif status == 2:
                # When add command is used and the key exists binary protocol notifies
                # the error as a key exists, this diverges from the ascii protocol that
                # returns a NOT_STORED. We try to make the everything compatible to the
                # ascii version
                return NOT_STORED
            elif status == 5:
                return NOT_STORED
            else:
                raise NotImplemented()
        finally:
            self._parser = None
