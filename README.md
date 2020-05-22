# emcache

A high performance asynchronous Python client for [Memcached](https://memcached.org/) with ~full~ almost full batteries included

[![Documentation Status](https://readthedocs.org/projects/emcache/badge/?version=latest)](https://emcache.readthedocs.io/en/latest/?badge=latest)


Emcache stands on giant's shoulders and implements most of the characteristics that are desired for a Memcached client based on the experience of other Memcached clients. It provides the following:

- Support for many Memcached hosts, distributing traffic around them by using the [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) algorithm.
- Support for different commands and different flag behaviors like `noreply`, `exptime` or `flags`.
- Adaptative connection pool, which increases the number of connections per Memcache host depending on the traffic.
- Speed, Emcache is fast. See the benchmark section.

Emcache is still on beta and has not bet yet tested in production but early feedback is wanted from the community to make it ready for production.

Full batteries must be finally provided in the following releases, the following characteristics will be added:

- Support for other commands, like `touch`, `delete`, etc.
- A public interface for managing the nodes of the cluster, allowing, for example, the addition or deletion of Memcached nodes at run time.
- Node healthiness support which would allow, optionable, removing nodes that are not responding.
- Cluster and node observability for retrieving stats or listening for significant events.

## Usage

For installing

```bash
pip install emcache
```

The following snippet shows the minimal stuff that would be needed for creating a new `emcache` client and saving a new key and retrieving later the value.

```python
import asyncio
import emcache
async def main():
    client = await emcache.create_client([('localhost', 11211)])
    await client.set(b'key', b'value')
    item = await client.get(b'key')
    print(item.value)

asyncio.run(main())
```

Emcache has currently support for the following commands:

- `get` Used for retrieving a specific key.
- `gets` Cas version that returns also the case token of a specific key.
- `get_many` Many keys get version.
- `gets_many` Many keys + case token gets version.
- `set` Set a new key and value
- `add` Add a new key and value, if and only if it does not exist.
- `replace` Update a value of a key, if an only if the key does exist.
- `append` Append a value to the current one for a specific key, if and only if the key does exist.
- `prepend` Prepend a value to the current one for a specific key, if and only if the key does exist.
- `cas` Update a value for a key if and only if token as provided matches with the ones stored in the Memcached server.

Some of the commands have support for the following behavior flags:

- `noreply` for storage commands like `set` we do not wait for an explicit response from the Memcached server. Sacrifice the explicit ack from the Memcached server for speed.
- `flags` for storage we can save an int16 value that can be retrieved later on by fetch commands.
- `exptime` for storage commands this provides a way of configuring an expiration time, once that time is reached keys will be automatically evicted by the Memcached server 

For more information about usage, [read the docs](https://emcache.readthedocs.io/en/latest/).


## Benchmarks

The following table shows how fast - operations per second - Emcache can be compared to the other two Memcached Python clients,
[aiomemcache](https://github.com/aio-libs/aiomcache) and [pymemcache](https://github.com/pinterest/pymemcache).
For that specific benchmark two nodes were used, one for the client and one for the Memcached server, using 32 TCP connections
and using 32 concurrent Asyncio tasks - threads for the use case of Pymemcache.

In the first part of the benchmark the client tried to run as much `set` operations it could, and in a second step the same was
done but using `get` operations.

| Client       | Concurrency    | Sets opS/sec  | Sets latency AVG  |  Gets opS/sec      | Gets latency AVG |
| ------------- | -------------:| -------------:| -----------------:|  -----------------:| ----------------:|
| aiomemcache   |            32 |         33872 |           0.00094 |              34183 |          0.00093 |
| pymemcache    |            32 |         32792 |           0.00097 |              32961 |          0.00096 |
| emcache       |            32 |         49410 |           0.00064 |              49212 |          0.00064 |

Emcache performed better than the other two implementations reaching almost 50K ops/sec for `get` and `set` operations.

Another benchmark was performed for comparing how each implementation will behave in case of having to deal with more than 1 node, a new
benchmark was performed with different cluster sizes but using the same methodology as the previous test by first, performing as many `set`
operations it could and later as many `get` operations it could. For this specific use test with Aiomemcahce could not be used since it
does not support multiple nodes.

| Client       | Concurrency   | Nodes | Sets opS/sec   | Sets latency AVG  |  Gets opS/sec       | Gets latency AVG |
| ------------- | -------------:| ----:| -------------:| ------------------:|  ------------------:| ----------------:|
| pymemcache    |            32 |    2 |         21260 |            0.00150 |               21583 |          0.00148 |
| emcache       |            32 |    2 |         42245 |            0.00075 |               48079 |          0.00066 |
| pymemcache    |            32 |    4 |         15334 |            0.00208 |               15458 |          0.00207 |
| emcache       |            32 |    4 |         39786 |            0.00080 |               47603 |          0.00067 |
| pymemcache    |            32 |    8 |          9903 |            0.00323 |                9970 |          0.00322 |
| emcache       |            32 |    8 |         42167 |            0.00075 |               46472 |          0.00068 |

The addition of new nodes did not add almost degradation for Emcache, in the last test with 8 nodes Emcache reached 42K
`get` ops/sec and 46K `set` ops/sec. On the other hand, Pymemcached suffered substantial degradation making Emcache ~x5 times.
faster.
