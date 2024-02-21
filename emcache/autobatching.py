# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
import logging
from typing import Awaitable, Callable, Dict, List, Optional

from ._cython import cyemcache
from .base import Client, Item
from .cluster import Cluster
from .node import Node
from .timeout import OpTimeout

logger = logging.getLogger(__name__)


class _InflightBatches:
    _batches: List[asyncio.Task]
    _ops: Dict[bytes, List[asyncio.Future]]
    _cluster: Cluster
    _loop: asyncio.AbstractEventLoop
    _command: bytes
    _return_flags: bool
    _return_cas: bool
    _timeout: Optional[float]
    _max_keys: int
    _on_finish: Optional[Callable[["_InflightBatches"], None]]

    def __init__(
        self,
        ops: Dict[bytes, List[asyncio.Future]],
        cluster: Cluster,
        on_finish: Callable[["_InflightBatches"], None],
        loop: asyncio.AbstractEventLoop,
        *,
        return_flags: bool,
        return_cas: bool,
        timeout: Optional[float],
        max_keys: int,
    ) -> None:
        self._ops = ops
        self._cluster = cluster
        self._on_finish = on_finish
        self._loop = loop
        self._batches = []
        self._command = b"gets" if return_cas else b"get"
        self._return_flags = return_flags
        self._return_cas = return_cas
        self._timeout = timeout
        self._max_keys = max_keys
        self._start()

    def __del__(self):
        if len(self._ops) > 0:
            logger.warning("Closing an inflight batch with pending operations")

    @property
    def size(self) -> int:
        return len(self._batches)

    def _start(self) -> None:
        # Distribute keys to the right nodes, and send batches of max_keys
        for node, keys in self._cluster.pick_nodes(self._ops.keys()).items():
            while keys:
                self._batches.append(self._loop.create_task(self._batch_operation(node, keys[: self._max_keys])))
                keys = keys[self._max_keys :]  # noqa

    async def _batch_operation(self, node: Node, keys: List[bytes]) -> None:
        try:
            async with OpTimeout(self._timeout, self._loop):
                async with node.connection() as connection:
                    results = await connection.fetch_command(self._command, keys)
        except asyncio.TimeoutError as err:
            # we need to raise this timeout to all of the desired waiters
            for key in keys:
                futures = self._ops.pop(key)
                for future in futures:
                    if not future.done():
                        future.set_exception(err)

            self._maybe_signal_termination()

            # nothing else needs to be done since we finished with a timeout
            # which invalidates the whole batch
            return

        returned_keys, values, flags, cas = results

        # First we wake up waiters which we got a result
        for idx in range(len(returned_keys)):
            value = values[idx]
            flags = flags[idx] if self._return_flags else None
            cas = cas[idx] if self._return_cas else None

            futures = self._ops.pop(returned_keys[idx])
            for future in futures:
                if not future.done():
                    future.set_result(Item(value, flags, cas))

        # Return a None for the missing keys
        for key in set(keys) - set(returned_keys):
            futures = self._ops.pop(key)
            for future in futures:
                if not future.done():
                    future.set_result(None)

        self._maybe_signal_termination()

    def _maybe_signal_termination(self) -> None:
        if len(self._ops) == 0:
            self._on_finish(self)


class AutoBatching:

    _inflight_batches: List[asyncio.Task]
    _pending_ops: Dict[bytes, List[asyncio.Future]]
    _next_loop_handler: Optional[asyncio.Handle]
    _client: Client
    _cluster: Cluster
    _loop: asyncio.AbstractEventLoop
    _cas: bool
    _return_flags: bool
    _timeout: Optional[float]
    _max_keys: int

    def __init__(
        self,
        client: Client,
        cluster: Cluster,
        loop: asyncio.AbstractEventLoop,
        *,
        return_flags: bool,
        return_cas: bool,
        timeout: Optional[bool],
        max_keys: int,
    ) -> None:
        self._pending_ops = {}
        self._inflight_batches = []
        self._next_loop_handler = None
        self._loop = loop
        self._client = client
        self._cluster = cluster
        self._return_flags = return_flags
        self._return_cas = return_cas
        self._timeout = timeout
        self._max_keys = max_keys

    def __del__(self):
        if self._pending_ops:
            logger.warning("Closing an autobatching instance with pending operations to be batched")

        if self._inflight_batches:
            logger.warning("Closing an autobatching instance with inflight batches")

    def execute(self, key: bytes) -> Awaitable[Item]:
        """Execute the Memcached operation, a get or a gets depending
        the parameters of the constuctor, as part of a batch which can
        contain many other operations.

        A Future is returned which will be waken up once the result is
        available.
        """
        if self._client.closed:
            raise RuntimeError("Emcache client is closed")

        if cyemcache.is_key_valid(key) is False:
            raise ValueError("Key has invalid charcters")

        future = self._loop.create_future()

        # During the same loop iteration we can have multiple
        # independent calls asking for the same key, we merge
        # together into a unique one but keeping the callers
        # independent.
        if key not in self._pending_ops:
            self._pending_ops[key] = [future]
        else:
            self._pending_ops[key].append(future)

        if self._next_loop_handler is None:
            # There is no yet scheduled a next loop iteration
            self._next_loop_handler = self._loop.call_soon(self._send_batches)

        return future

    def _send_batches(self) -> None:
        if self._client.closed:
            for futures in self._pending_ops.values():
                for future in futures:
                    if not future.done():
                        future.set_exception(RuntimeError("Emcache client is closed"))
        else:
            batch = _InflightBatches(
                self._pending_ops,
                self._cluster,
                self._on_finish_batch,
                self._loop,
                return_flags=self._return_flags,
                return_cas=self._return_cas,
                timeout=self._timeout,
                max_keys=self._max_keys,
            )
            self._inflight_batches.append(batch)

        # Ready for new batches
        self._next_loop_handler = None
        self._pending_ops = {}

    def _on_finish_batch(self, batch: _InflightBatches):
        self._inflight_batches.remove(batch)
