from enum import Enum


class Stats(str, Enum):
    settings = "settings"
    items = "items"
    sizes = "sizes"
    slabs = "slabs"
    conns = "conns"
