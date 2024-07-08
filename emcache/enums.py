import enum


class Watcher(enum.StrEnum):
    fetchers = "fetchers"
    mutations = "mutations"
    evictions = "evictions"
    connevents = "connevents"
    proxyreqs = "proxyreqs"
    proxyuser = "proxyuser"
    deletions = "deletions"
