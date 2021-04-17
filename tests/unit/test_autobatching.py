import asyncio
import itertools
from unittest.mock import ANY, Mock

import pytest
from asynctest import CoroutineMock, MagicMock as AsyncMagicMock

from emcache.autobatching import AutoBatching, _InflightBatches

pytestmark = pytest.mark.asyncio


class TestInfligthBartches:
    @pytest.fixture
    async def cluster(self):
        cluster = Mock()
        return cluster

    @pytest.fixture
    def ops(self, event_loop):
        # Operations are key, values where a key can have
        # multiple waiters (futures) waiting for a response
        # for the same key.
        return {
            b"key1": [event_loop.create_future(), event_loop.create_future()],
            b"key2": [event_loop.create_future()],
        }

    async def test_multiple_batches(self, event_loop, mocker, cluster, ops):
        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(return_value=(list(ops.keys()), list(ops.keys()), None, None))
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = itertools.chain(*list(ops.values()))

        # Configure batches of maximum one key per batch
        _ = _InflightBatches(
            ops, cluster, Mock(), event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        for future in futures:
            await future

        # Check that two calls to the fetch command were done
        assert connection.fetch_command.call_count == 2

    async def test_timeout_value_used(self, event_loop, mocker, cluster, ops):
        optimeout_class = mocker.patch("emcache.autobatching.OpTimeout", AsyncMagicMock())
        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(return_value=(list(ops.keys()), list(ops.keys()), None, None))
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = itertools.chain(*list(ops.values()))

        _ = _InflightBatches(
            ops, cluster, Mock(), event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        for future in futures:
            await future

        # Check that the timeout value was properly used
        optimeout_class.assert_called_with(1.0, ANY)

    async def test_futures_are_wake_up_no_side_effect_on_cancellation(self, event_loop, cluster, ops):
        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(return_value=(list(ops.keys()), list(ops.keys()), None, None))
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = list(itertools.chain(*list(ops.values())))

        # Cancel the first one
        futures[0].cancel()

        _ = _InflightBatches(
            ops, cluster, Mock(), event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        # Wait for the other ones
        for future in futures[1:]:
            await future

        # If cancellation would have not been handled properly we would have never
        # reach that point

    async def test_futures_missing_keys_are_wake_up_no_side_effect_on_cancellation(self, event_loop, cluster, ops):
        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(return_value=([], [], None, None))
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = list(itertools.chain(*list(ops.values())))

        # Cancel the first one
        futures[0].cancel()

        _ = _InflightBatches(
            ops, cluster, Mock(), event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        # Wait for the other ones
        for future in futures[1:]:
            await future

        # If cancellation would have not been handled properly we would have never
        # reach that point

    async def test_timeout_futures_are_wake_up(self, event_loop, mocker, cluster, ops):

        # force to trigger the exception at `fetch_command` level, thought is not the
        # where the exception will be raised is good enough for knowing if the caller
        # is managing as is expected the exception.
        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(side_effect=asyncio.TimeoutError)
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = itertools.chain(*list(ops.values()))

        _ = _InflightBatches(
            ops, cluster, Mock(), event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        # eventually futures will need to raise the proper exception
        for future in futures:
            with pytest.raises(asyncio.TimeoutError):
                await future

    async def test_timeout_futures_are_wake_up_no_side_effect_on_cancellation(self, event_loop, mocker, cluster, ops):

        # force to trigger the exception at `fetch_command` level, thought is not the
        # where the exception will be raised is good enough for knowing if the caller
        # is managing as is expected the exception.
        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(side_effect=asyncio.TimeoutError)
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = list(itertools.chain(*list(ops.values())))

        # Cancel the first one
        futures[0].cancel()

        _ = _InflightBatches(
            ops, cluster, Mock(), event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        # Wait for the other ones
        for future in futures[1:]:
            with pytest.raises(asyncio.TimeoutError):
                await future

        # If cancellation would not been well handled we will never reach that point

    async def test_signal_termination(self, event_loop, mocker, cluster, ops):
        on_finish = Mock()

        connection = CoroutineMock()
        connection.fetch_command = CoroutineMock(return_value=(list(ops.keys()), list(ops.keys()), None, None))
        node = Mock()
        connection_context = AsyncMagicMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        cluster.pick_nodes.return_value = {node: list(ops.keys())}

        # InflightBatches takes the ownership of the dictionary, we concat
        # all futures into our own list
        futures = itertools.chain(*list(ops.values()))

        inflight_batches = _InflightBatches(
            ops, cluster, on_finish, event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=1
        )

        for future in futures:
            await future

        # Check that the timeout value was properly used
        on_finish.assert_called_with(inflight_batches)


class TestAutoBatching:
    """ Only none happy path tests, happy path tests are
    covered as acceptance test.
    """

    @pytest.fixture
    async def client(self):
        client = Mock()
        client.closed = False
        return client

    @pytest.fixture
    async def cluster(self):
        cluster = Mock()
        return cluster

    @pytest.fixture
    async def autobatching(self, event_loop, client, cluster):
        return AutoBatching(client, cluster, event_loop, return_flags=False, return_cas=False, timeout=1.0, max_keys=32)

    async def test_get_invalid_key(self, autobatching):
        with pytest.raises(ValueError):
            await autobatching.execute(b"\n")

    async def test_get_client_closed(self, autobatching, client):
        client.closed = True
        with pytest.raises(RuntimeError):
            await autobatching.execute(b"key")

    async def test_client_closed_during_loop_iteration(self, autobatching, client):
        f = autobatching.execute(b"key")
        assert f.done() is False

        # lets close the client and wait one loop iteration
        client.closed = True
        await asyncio.sleep(0)

        # this should fail since client is already closed
        with pytest.raises(RuntimeError):
            await f

    async def test_inflight_batches_creation(self, mocker, event_loop, autobatching, cluster):
        inflight_batches_class = mocker.patch("emcache.autobatching._InflightBatches")

        # tigger an instantation after a loop iteration
        _ = autobatching.execute(b"key")
        await asyncio.sleep(0)

        inflight_batches_class.assert_called_with(
            # we do not take care of the ops provided during the unit test,
            # already covered by the acceptance test
            ANY,
            cluster,
            autobatching._on_finish_batch,
            event_loop,
            return_flags=False,
            return_cas=False,
            timeout=1.0,
            max_keys=32,
        )

    async def test_inflight_batches_noops(self, mocker, event_loop, autobatching, cluster):
        inflight_batches_class = mocker.patch("emcache.autobatching._InflightBatches")
        await asyncio.sleep(0)
        inflight_batches_class.assert_not_called()

    async def test_inflight_batches_multiple_oops(self, mocker, event_loop, autobatching, cluster):
        inflight_batches_class = mocker.patch("emcache.autobatching._InflightBatches")

        # tigger manyinstantation after and loop iterations
        for i in range(10):
            _ = autobatching.execute(b"key")
            await asyncio.sleep(0)

        assert inflight_batches_class.call_count == 10
