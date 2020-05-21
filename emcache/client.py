import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from ._cython import cyemcache
from .client_errors import NotStoredStorageCommandError, StorageCommandError
from .cluster import Cluster
from .default_values import DEFAULT_TIMEOUT
from .node import Node
from .protocol import EXISTS, NOT_STORED, STORED

logger = logging.getLogger(__name__)


MAX_ALLOWED_FLAG_VALUE = 2 ** 16
MAX_ALLOWED_CAS_VALUE = 2 ** 64


@dataclass
class Item:
    value: bytes
    flags: Optional[int]
    cas: Optional[int]


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

    def __init__(self, node_addresses: Sequence[Tuple[str, int]], timeout: float = DEFAULT_TIMEOUT) -> None:
        if not node_addresses:
            raise ValueError("At least one memcached hosts needs to be provided")

        self._loop = asyncio.get_running_loop()
        self._cluster = Cluster(node_addresses)
        self._timeout = timeout

    async def _storage_command(
        self, command: bytes, key: bytes, value: bytes, flags: int, exptime: int, noreply: bool, cas: int = None
    ) -> None:
        """ Proxy function used for all storage commands `add`, `set`,
        `replace`, `append` and `prepend`.
        """
        if cas is not None and cas > MAX_ALLOWED_CAS_VALUE:
            raise ValueError(f"flags can not be higher than {MAX_ALLOWED_FLAG_VALUE}")

        if flags > MAX_ALLOWED_FLAG_VALUE:
            raise ValueError(f"flags can not be higher than {MAX_ALLOWED_FLAG_VALUE}")

        if cyemcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with OpTimeout(self._timeout, self._loop):
            async with node.connection() as connection:
                return await connection.storage_command(command, key, value, flags, exptime, noreply, cas)

    async def _fetch_command(self, command: bytes, key: bytes) -> Optional[bytes]:
        """ Proxy function used for all fetch commands `get`, `gets`. """
        if cyemcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        node = self._cluster.pick_node(key)
        async with OpTimeout(self._timeout, self._loop):
            async with node.connection() as connection:
                return await connection.fetch_command(command, (key,))

    async def _fetch_many_command(
        self, command: bytes, keys: Sequence[bytes], return_flags=False
    ) -> Tuple[bytes, bytes, bytes]:
        """ Proxy function used for all fetch many commands `get_many`, `gets_many`. """
        if not keys:
            return {}

        for key in keys:
            if cyemcache.is_key_valid(key) is False:
                raise ValueError("Key has invalid charcters")

        async def node_operation(node: Node, keys: List[bytes]):
            async with node.connection() as connection:
                return await connection.fetch_command(command, keys)

        tasks = [
            self._loop.create_task(node_operation(node, keys)) for node, keys in self._cluster.pick_nodes(keys).items()
        ]

        async with OpTimeout(self._timeout, self._loop):
            try:
                await asyncio.gather(*tasks)
            except Exception:
                # Any exception will invalidate any ongoing
                # task.
                for task in tasks:
                    if not task.done():
                        task.cancel()
                raise

        return [task.result() for task in tasks]

    async def get(self, key: bytes, return_flags=False) -> Optional[Item]:
        """Return the value associated with the key as an `Item` instance.

        If `return_flags` is set to True, the `Item.flags` attribute will be
        set with the value saved along the value will be returned, otherwise
        a None value will be set.

        If key is not found, a `None` value will be returned.

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.
        """
        keys, values, flags, _ = await self._fetch_command(b"get", key)

        if key not in keys:
            return None

        if not return_flags:
            return Item(values[0], None, None)
        else:
            return Item(values[0], flags[0], None)

    async def gets(self, key: bytes, return_flags=False) -> Optional[Item]:
        """Return the value associated with the key and its cass value as
        an `Item` instance.

        If `return_flags` is set to True, the `Item.flags` attribute will be
        set with the value saved along the value will be returned, otherwise
        a None value will be set.

        If key is not found, a `None` value will be returned.

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.
        """
        keys, values, flags, cas = await self._fetch_command(b"gets", key)

        if key not in keys:
            return None

        if not return_flags:
            return Item(values[0], None, cas[0])
        else:
            return Item(values[0], flags[0], cas[0])

    async def get_many(self, keys: Sequence[bytes], return_flags=False) -> Dict[bytes, Item]:
        """Return the values associated with the keys.

        If a key is not found, the key won't be added to the result.

        More than one request could be sent concurrently to different nodes,
        where each request will be composed of one or many keys. Hashing
        algorithm will decide how keys will be grouped by.

        if any request fails due to a timeout - if it is configured - or any other
        error, all ongoing requests will be automatically canceled and the error will
        be raised back to the caller.
        """
        nodes_results = await self._fetch_many_command(b"get", keys, return_flags=return_flags)

        results = {}
        if not return_flags:
            for keys, values, flags, _ in nodes_results:
                for idx in range(len(keys)):
                    results[keys[idx]] = Item(values[idx], None, None)
        else:
            for keys, values, flags, _ in nodes_results:
                for idx in range(len(keys)):
                    results[keys[idx]] = Item(values[idx], flags[idx], None)

        return results

    async def gets_many(self, keys: Sequence[bytes], return_flags=False) -> Dict[bytes, Item]:
        """Return the values associated with the keys and their cas
        values.

        Take a look at the `get_many` command for parameters description.
        """
        nodes_results = await self._fetch_many_command(b"gets", keys, return_flags=return_flags)

        results = {}
        if not return_flags:
            for keys, values, flags, cas in nodes_results:
                for idx in range(len(keys)):
                    results[keys[idx]] = Item(values[idx], None, cas[idx])
        else:
            for keys, values, flags, cas in nodes_results:
                for idx in range(len(keys)):
                    results[keys[idx]] = Item(values[idx], flags[idx], cas[idx])

        return results

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

    async def cas(
        self, key: bytes, value: bytes, cas: int, *, flags: int = 0, exptime: int = 0, noreply: bool = False
    ) -> None:
        """Update a specific value for a given key using a cas
        value, if cas value does not match with the server one
        command will fail.

        If command fails a `StorageCommandError` is raised, however
        when `noreply` option is used there is no ack from the Memcached
        server, not raising any command error.

        Take a look at the `set` command for parameters description.
        use the documentation of that method.
        """
        result = await self._storage_command(b"cas", key, value, flags, exptime, noreply, cas=cas)

        if not noreply and result == EXISTS:
            raise NotStoredStorageCommandError()
        elif not noreply and result != STORED:
            raise StorageCommandError(f"Command finished with error, response returned {result}")
