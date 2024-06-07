# MIT License
# Copyright (c) 2020-2024 Pau Freixes

from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call

import pytest

from emcache import MemcachedHostAddress
from emcache.base import Item
from emcache.client import MAX_ALLOWED_CAS_VALUE, MAX_ALLOWED_FLAG_VALUE, _Client, create_client
from emcache.client_errors import CommandError, NotFoundCommandError, StorageCommandError
from emcache.default_values import (
    DEFAULT_AUTOBATCHING_ENABLED,
    DEFAULT_AUTOBATCHING_MAX_KEYS,
    DEFAULT_AUTODISCOVERY_POLL_INTERVAL,
    DEFAULT_AUTODISCOVERY_TIMEOUT,
    DEFAULT_CONNECTION_TIMEOUT,
    DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MIN_CONNECTIONS,
    DEFAULT_PURGE_UNHEALTHY_NODES,
    DEFAULT_PURGE_UNUSED_CONNECTIONS_AFTER,
    DEFAULT_SSL,
    DEFAULT_SSL_VERIFY,
    DEFAULT_TIMEOUT,
)
from emcache.protocol import DELETED, ERROR, NOT_FOUND, STORED, TOUCHED

pytestmark = pytest.mark.asyncio


class TestClient:
    """Only none happy path tests, happy path tests are
    covered as acceptance test.
    """

    @pytest.fixture
    async def cluster(self):
        cluster = Mock()
        cluster.close = AsyncMock()
        return cluster

    @pytest.fixture
    async def memcached_host_address(self):
        return MemcachedHostAddress(address="localhost", port=11211)

    @pytest.fixture
    async def client(self, event_loop, mocker, cluster, memcached_host_address):
        mocker.patch("emcache.client.Cluster", return_value=cluster)
        return _Client(
            [memcached_host_address],
            None,
            1,
            1,
            None,
            None,
            None,
            False,
            False,
            32,
            False,
            False,
            None,
            None,
            None,
            False,
            60,
            5,
        )

    async def test_invalid_host_addresses(self):
        with pytest.raises(ValueError):
            _Client([], None, 1, 1, None, None, None, False, False, 32, False, False, None, None, None, False, 60, 5)

    async def test_autobatching_initialization(self, event_loop, mocker, memcached_host_address):
        node_addresses = [memcached_host_address]
        timeout = 1
        max_connections = 1
        min_connections = 1
        purge_unused_connections_after = 60.0
        connection_timeout = 1.0
        cluster_events = Mock()
        purge_unhealthy_nodes = True
        autobatching = True
        autobatching_max_keys = 32
        ssl = False
        ssl_verify = False
        ssl_extra_ca = None
        username = None
        password = None
        autodiscovery = False
        autodiscovery_poll_interval = 60
        autodiscovery_timeout = 5
        cluster = Mock()
        cluster_class = mocker.patch("emcache.client.Cluster")
        cluster_class.return_value = cluster
        autobatching_class = mocker.patch("emcache.client.AutoBatching")
        client = _Client(
            node_addresses,
            timeout,
            max_connections,
            min_connections,
            purge_unused_connections_after,
            connection_timeout,
            cluster_events,
            purge_unhealthy_nodes,
            autobatching,
            autobatching_max_keys,
            ssl,
            ssl_verify,
            ssl_extra_ca,
            username,
            password,
            autodiscovery,
            autodiscovery_poll_interval,
            autodiscovery_timeout,
        )
        autobatching_class.assert_has_calls(
            [
                call(client, cluster, ANY, return_flags=False, return_cas=False, timeout=timeout, max_keys=32),
                call(client, cluster, ANY, return_flags=True, return_cas=False, timeout=timeout, max_keys=32),
                call(client, cluster, ANY, return_flags=False, return_cas=True, timeout=timeout, max_keys=32),
                call(client, cluster, ANY, return_flags=True, return_cas=True, timeout=timeout, max_keys=32),
            ]
        )

    async def test_cluster_initialization(self, event_loop, mocker, memcached_host_address):
        node_addresses = [memcached_host_address]
        timeout = 1
        max_connections = 1
        min_connections = 1
        purge_unused_connections_after = 60.0
        connection_timeout = 1.0
        cluster_events = Mock()
        purge_unhealthy_nodes = True
        autobatching = False
        autobatching_max_keys = 32
        ssl = False
        ssl_verify = False
        ssl_extra_ca = None
        username = None
        password = None
        autodiscovery = False
        autodiscovery_poll_interval = 60
        autodiscovery_timeout = 5
        cluster_class = mocker.patch("emcache.client.Cluster")
        _Client(
            node_addresses,
            timeout,
            max_connections,
            min_connections,
            purge_unused_connections_after,
            connection_timeout,
            cluster_events,
            purge_unhealthy_nodes,
            autobatching,
            autobatching_max_keys,
            ssl,
            ssl_verify,
            ssl_extra_ca,
            username,
            password,
            autodiscovery,
            autodiscovery_poll_interval,
            autodiscovery_timeout,
        )
        cluster_class.assert_called_with(
            node_addresses,
            max_connections,
            min_connections,
            purge_unused_connections_after,
            connection_timeout,
            cluster_events,
            purge_unhealthy_nodes,
            ssl,
            ssl_verify,
            ssl_extra_ca,
            username,
            password,
            autodiscovery,
            autodiscovery_poll_interval,
            autodiscovery_timeout,
            event_loop,
        )

    async def test_close(self, client, cluster):
        await client.close()
        await client.close()

        # under the hood cluster close method should be
        # called only once.
        cluster.close.assert_called_once()

    async def test_timeout_value_used(self, event_loop, mocker, memcached_host_address):
        mocker.patch("emcache.client.Cluster")

        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        timeout = 2.0
        client = _Client(
            [memcached_host_address],
            timeout,
            1,
            1,
            None,
            None,
            None,
            False,
            False,
            32,
            False,
            False,
            None,
            None,
            None,
            False,
            60,
            5,
        )
        connection = AsyncMock()
        connection.storage_command = AsyncMock(return_value=b"STORED")
        node = Mock()
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node

        await client.set(b"key", b"value")

        optimeout_class.assert_called_with(timeout, ANY)

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "prepend", "replace"])
    async def test_not_stored_error_storage_command(self, client, command):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.storage_command = AsyncMock(return_value=b"ERROR")
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(StorageCommandError):
            f = getattr(client, command)
            await f(b"foo", b"value")

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "prepend", "replace"])
    async def test_storage_command_invalid_key(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(b"\n", b"value")

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "prepend", "replace"])
    async def test_storage_command_client_closed(self, client, command):
        await client.close()
        with pytest.raises(RuntimeError):
            f = getattr(client, command)
            await f(b"key", b"value")

    @pytest.mark.parametrize("command", ["set", "add", "replace", "append", "prepend", "replace"])
    async def test_storage_command_use_timeout(self, client, command, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.storage_command = AsyncMock(return_value=STORED)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        f = getattr(client, command)
        await f(b"foo", b"value")

        optimeout_class.assert_called()

    async def test_cas_not_stored_error_storage_command(self, client):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.storage_command = AsyncMock(return_value=b"ERROR")
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(StorageCommandError):
            await client.cas(b"foo", b"value", 1)

    async def test_cas_use_timeout(self, client, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.storage_command = AsyncMock(return_value=STORED)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        await client.cas(b"foo", b"value", 1)

        optimeout_class.assert_called()

    async def test_cas_invalid_key(self, client):
        with pytest.raises(ValueError):
            await client.cas(b"\n", b"value", 1)

    async def test_cas_max_allowed_cas_value(self, client):
        with pytest.raises(ValueError):
            await client.cas(b"foo", b"value", MAX_ALLOWED_CAS_VALUE + 1)

    async def test_cas_max_allowed_flag_value(self, client):
        with pytest.raises(ValueError):
            await client.set(b"foo", b"value", flags=MAX_ALLOWED_FLAG_VALUE + 1)

    @pytest.mark.parametrize("command", ["get", "gets"])
    async def test_fetch_command_use_timeout(self, client, command, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.fetch_command = AsyncMock(return_value=iter([[b"foo"], [b"value"], [0], [0]]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        f = getattr(client, command)
        await f(b"foo")

        optimeout_class.assert_called()

    @pytest.mark.parametrize("command", ["gat", "gats"])
    async def test_get_and_touch_command_use_timeout(self, client, command, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.get_and_touch_command = AsyncMock(return_value=iter([[b"foo"], [b"value"], [0], [0]]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        f = getattr(client, command)
        await f(0, b"foo")

        optimeout_class.assert_called()

    @pytest.mark.parametrize("command", ["get", "gets"])
    async def test_fetch_command_invalid_key(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(b"\n")

    @pytest.mark.parametrize("command", ["gat", "gats"])
    async def test_get_and_touch_command_invalid_key(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(0, b"\n")

    @pytest.mark.parametrize("command", ["get", "gets"])
    async def test_fetch_command_client_closed(self, client, command):
        await client.close()
        with pytest.raises(RuntimeError):
            f = getattr(client, command)
            await f(b"key")

    @pytest.mark.parametrize("command", ["gat", "gats"])
    async def test_get_and_touch_command_client_closed(self, client, command):
        await client.close()
        with pytest.raises(RuntimeError):
            f = getattr(client, command)
            await f(0, b"key")

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_fetch_many_command_use_timeout(self, client, command, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.fetch_command = AsyncMock(return_value=iter([[b"foo"], [b"value"], [0], [0]]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_nodes.return_value = {node: [b"foo"]}
        f = getattr(client, command)
        await f([b"foo"])

        optimeout_class.assert_called()

    @pytest.mark.parametrize("command", ["gat_many", "gats_many"])
    async def test_get_and_touch_many_command_use_timeout(self, client, command, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.get_and_touch_command = AsyncMock(return_value=iter([[b"foo"], [b"value"], [0], [0]]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_nodes.return_value = {node: [b"foo"]}
        f = getattr(client, command)
        await f(0, [b"foo"])

        optimeout_class.assert_called()

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_fetch_many_command_empty_keys(self, client, command):
        f = getattr(client, command)
        result = await f([])
        assert result == {}

    @pytest.mark.parametrize("command", ["gat_many", "gats_many"])
    async def test_get_and_touch_many_command_empty_keys(self, client, command):
        f = getattr(client, command)
        result = await f(0, [])
        assert result == {}

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_fetch_many_command_invalid_keys(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f([b"\n"])

    @pytest.mark.parametrize("command", ["gat_many", "gats_many"])
    async def test_get_and_touch_many_command_invalid_keys(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(0, [b"\n"])

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_fetch_many_command_client_closed(self, client, command):
        await client.close()
        with pytest.raises(RuntimeError):
            f = getattr(client, command)
            await f([b"key"])

    @pytest.mark.parametrize("command", ["gat_many", "gats_many"])
    async def test_get_and_touch_many_command_client_closed(self, client, command):
        await client.close()
        with pytest.raises(RuntimeError):
            f = getattr(client, command)
            await f(0, [b"key"])

    @pytest.mark.parametrize("command", ["increment", "decrement"])
    async def test_incr_decr_use_timeout(self, client, command, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.incr_decr_command = AsyncMock(return_value=1)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        f = getattr(client, command)
        await f(b"foo", 1)

        optimeout_class.assert_called()

    @pytest.mark.parametrize("command", ["increment", "decrement"])
    async def test_incr_decr_invalid_key(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(b"\n", 1)

    @pytest.mark.parametrize("command", ["increment", "decrement"])
    async def test_incr_decr_invalid_value(self, client, command):
        with pytest.raises(ValueError):
            f = getattr(client, command)
            await f(b"\n", -1)

    @pytest.mark.parametrize("command", ["increment", "decrement"])
    async def test_incr_decr_client_closed(self, client, command):
        await client.close()
        with pytest.raises(RuntimeError):
            f = getattr(client, command)
            await f(b"key", 1)

    async def test_touch_invalid_key(self, client):
        with pytest.raises(ValueError):
            await client.touch(b"\n", 1)

    async def test_touch_client_closed(self, client):
        await client.close()
        with pytest.raises(RuntimeError):
            await client.touch(b"key", 1)

    async def test_touch_error_command(self, client):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.touch_command = AsyncMock(return_value=b"ERROR")
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(CommandError):
            await client.touch(b"foo", 1)

    async def test_touch_use_timeout(self, client, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.touch_command = AsyncMock(return_value=TOUCHED)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        await client.touch(b"foo", 1)

        optimeout_class.assert_called()

    async def test_delete_invalid_key(self, client):
        with pytest.raises(ValueError):
            await client.delete(b"\n")

    async def test_delete_client_closed(self, client):
        await client.close()
        with pytest.raises(RuntimeError):
            await client.delete(b"key")

    async def test_delete_error_command(self, client):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.delete_command = AsyncMock(return_value=iter([b"ERROR"]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(CommandError):
            await client.delete(b"foo")

    async def test_delete_error_not_found(self, client):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.delete_command = AsyncMock(return_value=NOT_FOUND)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        with pytest.raises(NotFoundCommandError):
            await client.delete(b"foo")

    async def test_delete_use_timeout(self, client, mocker):
        optimeout_class = mocker.patch("emcache.client.OpTimeout", MagicMock())

        connection = AsyncMock()
        connection.delete_command = AsyncMock(return_value=DELETED)
        connection_context = MagicMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node
        await client.delete(b"foo")

        optimeout_class.assert_called()

    async def test_flush_all_client_closed(self, client, memcached_host_address):
        await client.close()
        with pytest.raises(RuntimeError):
            await client.flush_all(memcached_host_address)

    async def test_flush_all_error_command(self, client, memcached_host_address):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.flush_all_command = AsyncMock(return_value=iter([b"ERROR"]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.node.return_value = node
        with pytest.raises(CommandError):
            await client.flush_all(memcached_host_address)

    @pytest.mark.parametrize("command", ["get_many", "gets_many"])
    async def test_exception_cancels_for_fetch_many(self, client, command):
        # patch what is necesary for rasing an exception for the first query and
        # a "valid" response from the others
        connection = AsyncMock()
        connection.fetch_command.side_effect = MagicMock(side_effect=iter([OSError(), b"Ok", b"Ok"]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node1 = Mock()
        node2 = Mock()
        node3 = Mock()
        node1.connection.return_value = connection_context
        node2.connection.return_value = connection_context
        node3.connection.return_value = connection_context
        client._cluster.pick_nodes.return_value = {node1: [b"key1"], node2: [b"key2"], node3: [b"key3"]}
        with pytest.raises(OSError):
            f = getattr(client, command)
            await f([b"key1", b"key2", b"key3"])

    @pytest.mark.parametrize("command", ["gat_many", "gats_many"])
    async def test_exception_cancels_for_get_and_touch_many(self, client, command):
        # patch what is necesary for rasing an exception for the first query and
        # a "valid" response from the others
        connection = AsyncMock()
        connection.get_and_touch_command.side_effect = MagicMock(side_effect=iter([OSError(), b"Ok", b"Ok"]))
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node1 = Mock()
        node2 = Mock()
        node3 = Mock()
        node1.connection.return_value = connection_context
        node2.connection.return_value = connection_context
        node3.connection.return_value = connection_context
        client._cluster.pick_nodes.return_value = {node1: [b"key1"], node2: [b"key2"], node3: [b"key3"]}
        with pytest.raises(OSError):
            f = getattr(client, command)
            await f(0, [b"key1", b"key2", b"key3"])

    @pytest.mark.parametrize("command", ["append", "prepend"])
    async def test_exptime_flags_disabled(self, client, command):
        # Some storage commands do not support update the flags and neither
        # the exptime, in these use cases the values are set to 0.
        connection = AsyncMock()
        connection.storage_command = AsyncMock(return_value=b"STORED")
        connection_context = MagicMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.pick_node.return_value = node

        f = getattr(client, command)
        await f(b"key", b"value")

        connection.storage_command.assert_called_with(ANY, ANY, ANY, 0, 0, ANY, ANY)

    @pytest.fixture
    async def autobatching(self, event_loop, mocker):
        item = Item(b"value", None, None)
        autobatching_get_noflags = MagicMock()
        autobatching_get_noflags.execute = AsyncMock(return_value=iter([item]))
        autobatching_get_flags = MagicMock()
        autobatching_get_flags.execute = AsyncMock(return_value=iter([item]))
        autobatching_gets_noflags = MagicMock()
        autobatching_gets_noflags.execute = AsyncMock(return_value=iter([item]))
        autobatching_gets_flags = MagicMock()
        autobatching_gets_flags.execute = AsyncMock(return_value=iter([item]))
        return (autobatching_get_noflags, autobatching_get_flags, autobatching_gets_noflags, autobatching_gets_flags)

    @pytest.fixture
    async def client_autobatching(self, event_loop, mocker, cluster, memcached_host_address, autobatching):
        autobatch_class = mocker.patch("emcache.client.AutoBatching")
        get_noflags, get_flags, gets_noflags, gets_flags = autobatching
        autobatch_class.side_effect = [get_noflags, get_flags, gets_noflags, gets_flags]
        mocker.patch("emcache.client.Cluster", return_value=cluster)
        return _Client(
            [memcached_host_address],
            None,
            1,
            1,
            None,
            None,
            None,
            False,
            True,
            32,
            False,
            False,
            None,
            None,
            None,
            False,
            60,
            5,
        )

    async def test_get_command_use_autobatching_if_enabled(self, client_autobatching, autobatching):
        await client_autobatching.get(b"foo")

        get_noflags, get_flags, gets_noflags, gets_flags = autobatching

        get_noflags.execute.assert_called_with(b"foo")
        get_flags.execute.assert_not_called()
        gets_noflags.execute.assert_not_called()
        gets_flags.execute.assert_not_called()

    async def test_get_and_flags_command_use_autobatching_if_enabled(self, client_autobatching, autobatching):
        await client_autobatching.get(b"foo", return_flags=True)

        get_noflags, get_flags, gets_noflags, gets_flags = autobatching

        get_flags.execute.assert_called_with(b"foo")
        get_noflags.execute.assert_not_called()
        gets_noflags.execute.assert_not_called()
        gets_flags.execute.assert_not_called()

    async def test_gets_command_use_autobatching_if_enabled(self, client_autobatching, autobatching):
        await client_autobatching.gets(b"foo")

        get_noflags, get_flags, gets_noflags, gets_flags = autobatching

        gets_noflags.execute.assert_called_with(b"foo")
        get_noflags.execute.assert_not_called()
        get_flags.execute.assert_not_called()
        gets_flags.execute.assert_not_called()

    async def test_gets_and_flags_command_use_autobatching_if_enabled(self, client_autobatching, autobatching):
        await client_autobatching.gets(b"foo", return_flags=True)

        get_noflags, get_flags, gets_noflags, gets_flags = autobatching

        gets_flags.execute.assert_called_with(b"foo")
        get_flags.execute.assert_not_called()
        get_noflags.execute.assert_not_called()
        gets_noflags.execute.assert_not_called()

    async def test_version_client_closed(self, client, memcached_host_address):
        await client.close()
        with pytest.raises(RuntimeError):
            await client.version(memcached_host_address)

    async def test_version_error_command(self, client, memcached_host_address):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.version_command = AsyncMock(return_value=b"ERROR")
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.node.return_value = node
        with pytest.raises(CommandError):
            await client.version(memcached_host_address)

    async def test_cache_memlimit_client_closed(self, client, memcached_host_address):
        await client.close()
        with pytest.raises(RuntimeError):
            await client.cache_memlimit(memcached_host_address, 64)

    async def test_cache_memlimit_error_command(self, client, memcached_host_address):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.cache_memlimit = AsyncMock(return_value=ERROR)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.node.return_value = node
        with pytest.raises(CommandError):
            await client.cache_memlimit(memcached_host_address, 64)

    async def test_stats_client_closed(self, client, memcached_host_address):
        await client.close()
        with pytest.raises(RuntimeError):
            await client.stats(memcached_host_address, "settings")

    async def test_stats_error_command(self, client, memcached_host_address):
        # patch what is necesary for returnning an error string
        connection = AsyncMock()
        connection.stats_command = AsyncMock(return_value=ERROR)
        connection_context = AsyncMock()
        connection_context.__aenter__.return_value = connection
        node = Mock()
        node.connection.return_value = connection_context
        client._cluster.node.return_value = node
        with pytest.raises(CommandError):
            await client.stats(memcached_host_address, "wrong")


async def test_create_client_default_values(event_loop, mocker):
    client_class = mocker.patch("emcache.client._Client")
    await create_client([("localhost", 11211)])
    client_class.assert_called_with(
        [("localhost", 11211)],
        DEFAULT_TIMEOUT,
        DEFAULT_MAX_CONNECTIONS,
        DEFAULT_MIN_CONNECTIONS,
        DEFAULT_PURGE_UNUSED_CONNECTIONS_AFTER,
        DEFAULT_CONNECTION_TIMEOUT,
        # No ClusterEVents provided
        None,
        DEFAULT_PURGE_UNHEALTHY_NODES,
        DEFAULT_AUTOBATCHING_ENABLED,
        DEFAULT_AUTOBATCHING_MAX_KEYS,
        DEFAULT_SSL,
        DEFAULT_SSL_VERIFY,
        None,
        None,
        None,
        False,
        DEFAULT_AUTODISCOVERY_POLL_INTERVAL,
        DEFAULT_AUTODISCOVERY_TIMEOUT,
    )
