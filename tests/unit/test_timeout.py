# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import asyncio
from unittest.mock import ANY, Mock

import pytest

from emcache.client import OpTimeout

pytestmark = pytest.mark.asyncio


class TestOpTimeout:
    async def test_timeout(self, event_loop):
        with pytest.raises(asyncio.TimeoutError):
            async with OpTimeout(0.01, event_loop):
                await asyncio.sleep(1)

    async def test_dont_timeout(self, event_loop):
        async with OpTimeout(1, event_loop):
            await asyncio.sleep(0.01)

    async def test_cancellation_is_supported(self, event_loop):
        ev = asyncio.Event()

        async def coro():
            async with OpTimeout(0.01, event_loop):
                ev.set()
                await asyncio.sleep(0.02)

        task = event_loop.create_task(coro())
        await ev.wait()

        # We cancel before the timeout is triggered
        task.cancel()

        # we should observe a cancellation rather than
        #  a timeout error.
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_check_cancel_timer_handler(self):
        # When a timeout is not triggered, time handler
        # must be cancelled

        timer_handler = Mock()
        loop = Mock()
        loop.call_later.return_value = timer_handler
        async with OpTimeout(0.01, loop):
            pass

        loop.call_later.assert_called_with(0.01, ANY)
        timer_handler.cancel.assert_called()

    async def test_check_cancel_timer_handler_when_exception_triggers(self):
        # the same but having an exception
        timer_handler = Mock()
        loop = Mock()
        loop.call_later.return_value = timer_handler

        with pytest.raises(Exception):
            async with OpTimeout(0.01, loop):
                raise Exception

        timer_handler.cancel.assert_called()
