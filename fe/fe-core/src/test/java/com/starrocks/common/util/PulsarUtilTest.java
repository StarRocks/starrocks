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

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.routineload.PulsarRoutineLoadJob;
import com.starrocks.proto.PPulsarMetaProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PulsarUtilTest {
    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    GlobalStateMgr globalStateMgr;
    @Mocked
    SystemInfoService service;
    @Mocked
    BackendServiceClient client;

    @BeforeEach
    public void before() throws StarRocksException {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                BackendServiceClient.getInstance();
                minTimes = 0;
                result = client;
            }
        };
        UtFrameUtils.mockInitWarehouseEnv();
    }

    @Test
    public void testNoAliveComputeNode() throws StarRocksException {
        new MockUp<MockedWarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                return Lists.newArrayList();
            }
        };
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L,
                "http://pulsar-service", "topic1", "sub1");
        Deencapsulation.setField(job, "convertedCustomProperties", ImmutableMap.of("key1", "value1"));
        LoadException e = Assertions.assertThrows(LoadException.class, () -> job.getAllPulsarPartitions());
        Assertions.assertTrue(e.getMessage().contains("No alive backends or computeNodes"));
    }

    @Test
    public void testWithAliveComputeNode() throws StarRocksException, RpcException {
        new MockUp<MockedWarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                return Lists.newArrayList(1L);
            }
        };

        StatusPB status = new com.starrocks.proto.StatusPB();
        status.setStatusCode(0);
        PPulsarProxyResult proxyResult = new PPulsarProxyResult();
        proxyResult.setStatus(status);
        PPulsarMetaProxyResult pulsarMetaResult = new PPulsarMetaProxyResult();
        List<String> partitions = Lists.newArrayList("partition1", "partition2");
        pulsarMetaResult.setPartitions(partitions);
        proxyResult.setPulsarMetaResult(pulsarMetaResult);
        new Expectations() {
            {
                client.getPulsarInfo((TNetworkAddress) any, (PPulsarProxyRequest) any);
                result = CompletableFuture.completedFuture(proxyResult);
            }
        };
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L,
                "http://pulsar-service", "topic1", "sub1");
        Deencapsulation.setField(job, "convertedCustomProperties", ImmutableMap.of("key1", "value1"));

        List<String> result = job.getAllPulsarPartitions();
        Assertions.assertEquals(partitions, result);
    }
}
