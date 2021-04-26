0.6.0
================
- Support for SSL [#62](https://github.com/emcache/emcache/pull/62)

0.5.0
================
- Support for autobatching [#57](https://github.com/emcache/emcache/pull/57)

0.4.1
======
- Add create connection latencies metrics [#54](https://github.com/emcache/emcache/pull/54)
- Add new `min_connections` parameter [#55](https://github.com/emcache/emcache/pull/55)
- Purge only one connection per loop iteration [#56](https://github.com/emcache/emcache/pull/56)

0.4.0
==========
- Stable version

0.3.2b0
==========
- Support for Python 3.9 [#52](https://github.com/emcache/emcache/pull/52)

0.3.1b0
==========
- Fix bug: release connection when waiter is cancelled after wakeup [#48](https://github.com/emcache/emcache/issues/48)

0.3.0b0
=======
- Add support for the `increment` and `decrement` commands [#36](https://github.com/emcache/emcache/pull/36)
- Add support for the `touch` command [#37](https://github.com/emcache/emcache/pull/37)
- Add support for `flush_all` command [#38](https://github.com/emcache/emcache/pull/38)
- Add support for `delete` command [#39](https://github.com/emcache/emcache/pull/39)

0.2.1b0
=======
- Add new close method at Client class level for releasing any underlying resource used. [#34](https://github.com/emcache/emcache/pull/34)

0.2.0b0
=======
- Try to have always at least one TCP connection per host. [#26](https://github.com/emcache/emcache/pull/26)
- Quadratic backoff until reach the maximum of 60 seconds when an attempt for openning a connection
  fails. [#26](https://github.com/emcache/emcache/pull/26)
- Support for tracking the healthiness of the nodes, by checking that always at least there is
  one TCP connection. Unhealthy nodes can be optionally removed from pool of nodes elegible for sending
  traffic. [#27](https://github.com/emcache/emcache/pull/27)
- Support for cluster events for telling you in real time what signfificant events are happening,
  for now only supports two events for telling you when a node has changed the healthy status. [#27](https://github.com/emcache/emcache/pull/27)[#32](https://github.com/emcache/emcache/pull/32)
- Support for cluster managment which provies different operations for the cluster like listing the nodes
  that are participating into the cluster, or return the ones that are healthy or unhealthy. [#29](https://github.com/emcache/emcache/pull/29)
- New cluster managment function for returning the most important metrics observed at connection pool
  level [#30](https://github.com/emcache/emcache/pull/30)

0.1.1b0
=======
- Disabled support for `exptime` and `flags` for the `append` and `prepend` commands. For both commands
  Memcached ignores their value. [#25](https://github.com/emcache/emcache/pull/25)

0.1.0b0
=======
First release
