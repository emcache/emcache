# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
from typing import Optional


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
