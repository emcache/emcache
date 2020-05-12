import asyncio
import logging
from typing import Optional

from ._cython import cyfastcache
from .cluster import Cluster
from .default_values import DEFAULT_TIMEOUT

logger = logging.getLogger(__name__)


class CommandErrorException(Exception):
    """Exception raised when a command finished with error."""

    pass


class OpTimeout:

    _timeout: Optional[float]
    _loop: asyncio.AbstractEventLoop
    _task: Optional[asyncio.Task]
    _timed_out: bool
    _timer_handler: Optional[asyncio.TimerHandle]

    __slots__ = ("_timed_out", "_timeout", "_loop", "_task", "_timer_handler")

    def __init__(self, timeout: Optional[float], loop):
        self._timed_out = False
        self._timeout = timeout
        self._loop = loop
        self._task = asyncio.current_task(loop)
        self._timer_handler = None

    def _on_timeout(self):
        if not self._task.done():
            self._timed_out = True
            self._task.cancel()

    async def __aenter__(self):
        if self._timeout is not None:
            self._timer_handler = self._loop.call_later(self._timeout, self._on_timeout)

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._timed_out and (exc_type == asyncio.CancelledError):
            # its not a real cancellation, was a timeout
            raise asyncio.TimeoutError

        if self._timer_handler:
            self._timer_handler.cancel()


class Client:

    _cluster: Cluster
    _timeout: Optional[float]
    _loop: asyncio.AbstractEventLoop

    def __init__(self, host: str, port: int, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._loop = asyncio.get_running_loop()
        self._cluster = Cluster([(host, port)])
        self._timeout = timeout

    async def get(self, key: bytes) -> Optional[bytes]:
        """Return the value associated with the key.

        If key is not found, a `None` value will be returned.

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.
        """
        if cyfastcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with OpTimeout(self._timeout, self._loop):
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

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.
        """

        if cyfastcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with OpTimeout(self._timeout, self._loop):
            async with node.connection() as connection:
                result = await connection.set_cmd(key, value)

        if result != b"STORED":
            raise CommandErrorException("set command finished with error, response returned {result}")

        return result
