from dataclasses import dataclass


@dataclass(frozen=True)
class MemcachedHostAddress:
    """Data class for identifying univocally a Memcached host."""

    address: str
    port: int


@dataclass(frozen=True)
class MemcachedUnixSocketPath:
    path: str  # TODO: normalize path (make it absolute?)
