# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import pytest

from emcache import ConnectionPoolMetrics

pytestmark = pytest.mark.asyncio


class TestClusterManagment:
    async def test_nodes(self, client, memcached_address_1, memcached_address_2):
        assert client.cluster_managment().nodes() == [memcached_address_1, memcached_address_2]

    async def test_healthy_nodes(self, client, memcached_address_1, memcached_address_2):
        assert client.cluster_managment().healthy_nodes() == [memcached_address_1, memcached_address_2]

    async def test_unhealthy_nodes(self, client, memcached_address_1, memcached_address_2):
        assert client.cluster_managment().unhealthy_nodes() == []

    async def test_connection_pool_metrics(self, client, memcached_address_1, memcached_address_2):
        metrics = client.cluster_managment().connection_pool_metrics()

        # just check that both nodes appear in the dictionary returned, more accurated
        # tests for metrics attributes are done as unit tests
        assert isinstance(metrics[memcached_address_1], ConnectionPoolMetrics)
        assert isinstance(metrics[memcached_address_2], ConnectionPoolMetrics)
