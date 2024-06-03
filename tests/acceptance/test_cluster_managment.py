# MIT License
# Copyright (c) 2020-2024 Pau Freixes

import pytest

from emcache import ConnectionPoolMetrics

pytestmark = pytest.mark.asyncio


class TestClusterManagment:
    async def test_nodes(self, client, node_addresses):
        assert client.cluster_managment().nodes() == node_addresses

    async def test_healthy_nodes(self, client, node_addresses):
        assert client.cluster_managment().healthy_nodes() == node_addresses

    async def test_unhealthy_nodes(self, client):
        assert client.cluster_managment().unhealthy_nodes() == []

    async def test_connection_pool_metrics(self, client, node_addresses):
        metrics = client.cluster_managment().connection_pool_metrics()

        # just check that both nodes appear in the dictionary returned, more accurated
        # tests for metrics attributes are done as unit tests
        for node_address in node_addresses:
            assert isinstance(metrics[node_address], ConnectionPoolMetrics)
