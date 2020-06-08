0.2.0b1
=======
- Add new close method at Client class level for releasing any underlying resource used.

0.2.0b0
=======
- Try to have always at least one TCP connection per host. [#26](https://github.com/pfreixes/emcache/pull/26)
- Quadratic backoff until reach the maximum of 60 seconds when an attempt for openning a connection
  fails. [#26](https://github.com/pfreixes/emcache/pull/26)
- Support for tracking the healthiness of the nodes, by checking that always at least there is
  one TCP connection. Unhealthy nodes can be optionally removed from pool of nodes elegible for sending
  traffic. [#27](https://github.com/pfreixes/emcache/pull/27)
- Support for cluster events for telling you in real time what signfificant events are happening,
  for now only supports two events for telling you when a node has changed the healthy status. [#27](https://github.com/pfreixes/emcache/pull/27)[#32](https://github.com/pfreixes/emcache/pull/32)
- Support for cluster managment which provies different operations for the cluster like listing the nodes
  that are participating into the cluster, or return the ones that are healthy or unhealthy. [#29](https://github.com/pfreixes/emcache/pull/29)
- New cluster managment function for returning the most important metrics observed at connection pool
  level [#30](https://github.com/pfreixes/emcache/pull/30)

0.1.1b0
=======
- Disabled support for `exptime` and `flags` for the `append` and `prepend` commands. For both commands
  Memcached ignores their value. [#25](https://github.com/pfreixes/emcache/pull/25)

0.1.0b0
=======
First release
