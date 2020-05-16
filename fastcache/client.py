import asyncio
import logging
from typing import Optional

from ._cython import cyfastcache
from .client_errors import NotStoredStorageCommandError, StorageCommandError
from .cluster import Cluster
from .default_values import DEFAULT_TIMEOUT
from .protocol import NOT_STORED, STORED

logger = logging.getLogger(__name__)


MAX_ALLOWED_FLAG_VALUE = 2 ** 16


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

    async def _storage_command(
        self, command: bin, key: bin, value: bin, flags: int, exptime: int, noreply: bool
    ) -> None:
        """ Intermediate function used for all storage commands `add`, `set`,
        `replace`, `append` and `prepend`.
        """
        if flags > MAX_ALLOWED_FLAG_VALUE:
            raise ValueError(f"flags can not be higher than {MAX_ALLOWED_FLAG_VALUE}")

        if cyfastcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with OpTimeout(self._timeout, self._loop):
            async with node.connection() as connection:
                return await connection.storage_command(command, key, value, flags, exptime, noreply)

    async def get(self, key: bytes, return_flags=False) -> Optional[bytes]:
        """Return the value associated with the key.

        If `return_flags` is set to True, a tuple with the value
        and the flags that were saved along the value will be returned.

        If key is not found, a `None` value will be returned. If
        `return_flags` is set to True, a tuple with two `Nones` will
        be returned.

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.
        """
        if cyfastcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with OpTimeout(self._timeout, self._loop):
            async with node.connection() as connection:
                keys, values, flags = await connection.get_cmd(key)

        if key not in keys:
            if not return_flags:
                return None
            else:
                return None, None
        else:
            if not return_flags:
                return values[0]
            else:
                return values[0], flags[0]

    async def set(self, key: bytes, value: bytes, *, flags: int = 0, exptime: int = 0, noreply: bool = False) -> None:
        """Set a specific value for a given key.

        If command fails a `StorageCommandError` is raised, however
        when `noreply` option is used there is no ack from the Memcached
        server, not raising any command error.

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.

        Other parameters are optional, use them in the following
        situations:

        - `flags` is an arbitrary 16-bit unsigned integer stored
        along the value that can be retrieved later with a retrieval
        command.
        - `exptime` is the expiration time expressed as an aboslute
        timestamp. By default, it is set to 0 meaning that the there
        is no expiration time.
        - `noreply` when is set memcached will not return a response
        back telling how the opreation finished, avoiding a full round
        trip between the client and sever. By using this, the client
        won't have an explicit way for knowing if the storage command
        finished correctly. By default is disabled.
        """
        result = await self._storage_command(b"set", key, value, flags, exptime, noreply)

        if not noreply and result != STORED:
            raise StorageCommandError(f"Command finished with error, response returned {result}")

    async def add(self, key: bytes, value: bytes, *, flags: int = 0, exptime: int = 0, noreply: bool = False) -> None:
        """Set a specific value for a given key if and only if the key
        does not already exist.

        If the command fails because the key already exists a
        `NotStoredStorageCommandError` exception is raised, for other
        errors the generic `StorageCommandError` is used. However when
        `noreply` option is used there is no ack from the Memcached
        server, not raising any command error.

        Take a look at the `set` command for parameters description.
        """
        result = await self._storage_command(b"add", key, value, flags, exptime, noreply)

        if not noreply and result == NOT_STORED:
            raise NotStoredStorageCommandError()
        elif not noreply and result != STORED:
            raise StorageCommandError(f"Command finished with error, response returned {result}")

    async def replace(
        self, key: bytes, value: bytes, *, flags: int = 0, exptime: int = 0, noreply: bool = False
    ) -> None:
        """Set a specific value for a given key if and only if the key
        already exists.

        If the command fails because the key was not found a
        `NotStoredStorageCommandError` exception is raised, for other
        errors the generic `StorageCommandError` is used. However when
        `noreply` option is used there is no ack from the Memcached
        server, not raising any command error.

        Take a look at the `set` command for parameters description.
        """
        result = await self._storage_command(b"replace", key, value, flags, exptime, noreply)

        if not noreply and result == NOT_STORED:
            raise NotStoredStorageCommandError()
        elif not noreply and result != STORED:
            raise StorageCommandError(f"Command finished with error, response returned {result}")

    async def append(
        self, key: bytes, value: bytes, *, flags: int = 0, exptime: int = 0, noreply: bool = False
    ) -> None:
        """Append a specific value for a given key to the current value
        if and only if the key already exists.

        If the command fails because the key was not found a
        `NotStoredStorageCommandError` exception is raised, for other
        errors the generic `StorageCommandError` is used. However when
        `noreply` option is used there is no ack from the Memcached
        server, not raising any command error.

        Take a look at the `set` command for parameters description.
        """
        result = await self._storage_command(b"append", key, value, flags, exptime, noreply)

        if not noreply and result == NOT_STORED:
            raise NotStoredStorageCommandError()
        elif not noreply and result != STORED:
            raise StorageCommandError(f"Command finished with error, response returned {result}")

    async def prepend(
        self, key: bytes, value: bytes, *, flags: int = 0, exptime: int = 0, noreply: bool = False
    ) -> None:
        """Prepend a specific value for a given key to the current value
        if and only if the key already exists.

        If the command fails because the key was not found a
        `NotStoredStorageCommandError` exception is raised, for other
        errors the generic `StorageCommandError` is used. However when
        `noreply` option is used there is no ack from the Memcached
        server, not raising any command error.

        Take a look at the `set` command for parameters description.
        use the documentation of that method.
        """
        result = await self._storage_command(b"prepend", key, value, flags, exptime, noreply)

        if not noreply and result == NOT_STORED:
            raise NotStoredStorageCommandError()
        elif not noreply and result != STORED:
            raise StorageCommandError(f"Command finished with error, response returned {result}")
