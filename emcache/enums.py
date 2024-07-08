from enum import Enum


class Watcher(str, Enum):
    fetchers = "fetchers"
    mutations = "mutations"
    evictions = "evictions"
    connevents = "connevents"
    proxyreqs = "proxyreqs"
    proxyuser = "proxyuser"
    deletions = "deletions"
