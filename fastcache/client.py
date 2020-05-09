import asyncio
import logging
from typing import Optional

from ._cython import cyfastcache
from .cluster import Cluster

logger = logging.getLogger(__name__)


class CommandErrorException(Exception):
    """Exception raised when a command finished with error."""

    pass


class Client:

    _cluster: Cluster

    def __init__(self, host: str, port: int) -> None:
        self._cluster = Cluster([(host, port)])

    async def get(self, key: bytes) -> Optional[bytes]:
        """Return the value associated with the key.

        If key is not found, a None value will be returned.
        """
        if cyfastcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with node.connection() as connection:
            keys, values = await connection.get_cmd(key)

        if key not in keys:
            return None
        else:
            return values[0]

    async def set(self, key: bytes, value: bytes) -> bool:
        """Set a specific value for a given key.
        
        Returns True if the key was stored succesfully, otherwise
        a `CommandErrorException` is raised.
        """

        if cyfastcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with node.connection() as connection:
            result = await connection.set_cmd(key, value)

        if result != b"STORED":
            raise CommandErrorException("set command finished with error, response returned {result}")

        return result
