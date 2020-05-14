class StorageCommandError(Exception):
    """ General exception raised when a storage command finished without
    being able to store the value for a specific key."""

    pass


class NotStoredStorageCommandError(StorageCommandError):
    """ Explicitly says that the value was not sotred, this exception
    is typically raised when conditions are not meet for the `add`,
    `replace` and other storage commands that they need the presense
    or abscence of a key.
    """

    pass
