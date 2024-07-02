# MIT License
# Copyright (c) 2020-2024 Pau Freixes


class ClusterNoAvailableNodes(Exception):
    """Error raised from the cluster when there is no nodes
    available in the cluster, because all of them are unhealthy
    and purged or because they were removed.
    """


class CommandError(Exception):
    """Base exception Memcached commands"""


class StorageCommandError(CommandError):
    """General exception raised when a storage command finished without
    being able to store the value for a specific key.
    """


class NotStoredStorageCommandError(StorageCommandError):
    """Explicitly says that the value was not sotred, this exception
    is typically raised when conditions are not meet for the `add`,
    `replace` and other storage commands that they need the presense
    or abscence of a key.
    """


class NotFoundCommandError(CommandError):
    """When a key does not exist some commands can not perform
    the operation and this exception is raised.
    """


class AuthenticationError(CommandError):
    """An exception is thrown when authentication was failed"""
