from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from .node import MemcachedHostAddress


@dataclass
class Item:
    value: bytes
    flags: Optional[int]
    cas: Optional[int]


class Client(metaclass=ABCMeta):
    @abstractmethod
    async def get(self, key: bytes, return_flags=False) -> Optional[Item]:
        """Return the value associated with the key as an `Item` instance.

        If `return_flags` is set to True, the `Item.flags` attribute will be
        set with the value saved along the value will be returned, otherwise
        a None value will be set.

        If key is not found, a `None` value will be returned.

        If timeout is not disabled, an `asyncio.TimeoutError` will
        be returned in case of a timed out operation.
        """

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    async def gets_many(self, keys: Sequence[bytes], return_flags=False) -> Dict[bytes, Item]:
        """Return the values associated with the keys and their cas
        values.

        Take a look at the `get_many` command for parameters description.
        """

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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


class ClusterEvents(metaclass=ABCMeta):
    """ ClusterEvents can be used for being notified about different
    events that happen at cluster level.

    Each kind of event is identified with its own function named
    `on_<event_name>` which might be called zero, one or many times.
    """

    @abstractmethod
    async def on_node_healthy(self, cluster_managment: "ClusterManagment", host: MemcachedHostAddress) -> None:
        """Called when a node is marked as healthy.

        A node is marked as healthy when there is at least one TCP
        connection oppened to the host.
        """

    @abstractmethod
    async def on_node_unhealthy(self, cluster_managment: "ClusterManagment", host: MemcachedHostAddress) -> None:
        """Called when a new node is marked as umhealthy.

        A node is marked as unhealthy when there is no TCP
        connection oppened to the host and the last attempts for
        oppening one have failed.

        Traffic might no be longer routed to that host depending
        on the cluster configuration, take a look to the
        `purge_unhealthy_nodes` parameter provided during the
        client creation.

        These event will be fired in any circumstance without depending on
        the value of the `purge_unhealthy_nodes` value.
        """


class ClusterManagment(metaclass=ABCMeta):
    """ ClusterManagment provides you the public interface
    for managing the cluster.

    A `Client` instance proides you a way for having access
    to an instance of `ClusterManagment` related to the cluster
    used for that specific client, as the following example
    shows:

        >>> client = await emcache.create_client(...)
        >>> cluster_managment = client.cluster_managment()
        >>> print(cluster_managment.nodes())

    Take a look to the different methods for knowing what operations
    are currently supported.
    """

    @abstractmethod
    def nodes(self) -> Sequence[MemcachedHostAddress]:
        """Return the nodes that belong to the cluster. """

    @abstractmethod
    def healthy_nodes(self) -> Sequence[MemcachedHostAddress]:
        """Return the nodes that are considered healthy. """

    @abstractmethod
    def unhealthy_nodes(self) -> Sequence[MemcachedHostAddress]:
        """Return the nodes that are considered unhealthy. """
