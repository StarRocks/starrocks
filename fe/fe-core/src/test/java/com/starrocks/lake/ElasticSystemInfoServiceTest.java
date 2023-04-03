// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.lake;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ElasticSystemInfoServiceTest {
    private SystemInfoService service;
    private long clusterId;

    private long backendId0;
    private long backendId1;
    private long backendId2;
    private Backend backend0;
    private Backend backend1;
    private Backend backend2;

    @Mocked
    private StarOSAgent agent;

    @Before
    public void setUp() {
        service = new ElasticSystemInfoService(agent);
        clusterId = 1L;

        backendId0 = 10000L;
        backendId1 = 10001L;
        backendId2 = 10002L;

        backend0 = new Backend(backendId0, "127.0.0.1", 9050);
        backend0.setIsAlive(true);
        backend0.setBePort(9060);
        backend0.setHttpPort(8040);
        backend0.setBeRpcPort(8060);

        backend1 = new Backend(backendId1, "127.0.0.2", 9051);
        backend1.setIsAlive(false);
        backend1.setBePort(9061);
        backend1.setHttpPort(8041);
        backend1.setBeRpcPort(8061);

        backend2 = new Backend(backendId2, "127.0.0.3", 9052);
        backend2.setIsAlive(true);
        backend2.setBePort(9062);
        backend2.setHttpPort(8042);
        backend2.setBeRpcPort(8062);
    }

    @Test
    public void testGetBackend() throws Exception {
        new Expectations() {
            {
                agent.getWorkerById(backendId0);
                minTimes = 0;
                result = backend0;
                agent.getWorkerById(backendId1);
                minTimes = 0;
                result = backend1;
                agent.getWorkerById(backendId2);
                minTimes = 0;
                result = backend2;
                agent.getWorkerById(10003L);
                minTimes = 0;
                result = null;

                agent.getWorkersByWorkerGroup(Arrays.asList(clusterId));
                minTimes = 0;
                result = Lists.newArrayList(backend0, backend1);
                agent.getWorkers();
                minTimes = 0;
                result = Lists.newArrayList(backend0, backend1, backend2);
            }
        };

        Assert.assertFalse(service.isSingleBackendAndComputeNode(clusterId));

        Backend backend = service.getBackend(backendId2);
        Assert.assertEquals("127.0.0.3", backend.getHost());
        Assert.assertTrue(service.checkBackendAvailable(backendId0));
        Assert.assertFalse(service.checkBackendAvailable(10003L));
        Assert.assertFalse(service.checkBackendAlive(backendId1));

        backend = service.getBackendWithBePort("127.0.0.1", 9060);
        Assert.assertEquals(backendId0, backend.getId());
        TNetworkAddress address = service.toBrpcHost(new TNetworkAddress(backend.getHost(), backend.getBePort()));
        Assert.assertEquals(backend.getHost(), address.getHostname());
        Assert.assertEquals(backend.getBrpcPort(), address.getPort());

        Assert.assertEquals(3, service.getTotalBackendNumber());
        Assert.assertEquals(2, service.getTotalBackendNumber(clusterId));
        Assert.assertEquals(2, service.getBackendIds(true).size());
        Assert.assertEquals(1, service.getBackendIds(true, clusterId).size());

        List<Backend> backends = service.getBackends(clusterId);
        Assert.assertEquals(2, backends.size());
        for (Backend be : backends) {
            Assert.assertTrue(be.getId() == backendId0 || be.getId() == backendId1);
        }

        ImmutableMap<Long, Backend> idToBackends = service.getIdToBackend(clusterId);
        Assert.assertEquals(2, idToBackends.size());
        Assert.assertTrue(idToBackends.containsKey(backendId0) && idToBackends.containsKey(backendId1));
    }
}