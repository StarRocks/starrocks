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
import com.starrocks.proto.PKafkaMetaProxyResult;
import com.starrocks.proto.PKafkaOffsetProxyResult;
import com.starrocks.proto.PProxyRequest;
import com.starrocks.proto.PProxyResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaUtilTest {
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

        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                return Lists.newArrayList(1L);
            }
        };

        UtFrameUtils.mockInitWarehouseEnv();
    }

    @Test
    public void testNoAliveComputeNode() throws StarRocksException {
        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = null;
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assertions.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assertions.assertEquals(
                "Failed to send get kafka partition info request. err: No alive backends or compute nodes",
                e.getMessage());
    }

    @Test
    public void testGetInfoRpcException() throws StarRocksException, RpcException {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);

        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new RpcException("rpc failed");
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assertions.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assertions.assertTrue(e.getMessage().contains("err: rpc failed"));
    }

    @Test
    public void testGetInfoInterruptedException() throws StarRocksException, RpcException {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);

        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;

                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new InterruptedException("interrupted");
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assertions.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assertions.assertTrue(e.getMessage().contains("Got interrupted exception"));
    }

    @Test
    public void testGetInfoValidateObjectException() throws StarRocksException, RpcException {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);

        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new RpcException("Unable to validate object");
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assertions.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assertions.assertTrue(e.getMessage().contains("err: BE is not alive"));
    }

    @Test
    public void testGetInfoFailed() throws StarRocksException, RpcException {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);

        PProxyResult proxyResult = new PProxyResult();
        StatusPB status = new StatusPB();
        // cancelled
        status.statusCode = 1;
        status.errorMsgs = Lists.newArrayList("be process failed");
        proxyResult.status = status;

        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new Future<PProxyResult>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public PProxyResult get() throws InterruptedException, ExecutionException {
                        return proxyResult;
                    }

                    @Override
                    public PProxyResult get(long timeout, @NotNull TimeUnit unit)
                            throws InterruptedException, ExecutionException, TimeoutException {
                        return proxyResult;
                    }
                };
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assertions.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assertions.assertTrue(e.getMessage().contains("be process failed"));
    }

    @Test
    public void testWarehouseNotExist() {
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };
        mockedWarehouseManager.setThrowUnknownWarehouseException();

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assertions.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assertions.assertEquals("Failed to send get kafka partition info request. err: " +
                "Warehouse id: 1 not exist.", e.getMessage());
    }

    @Test
    public void getOffsets_returnsCorrectOffsetsForLatest() throws StarRocksException, RpcException {
        List<Integer> partitions = Lists.newArrayList(0, 1, 2);
        List<Long> latestOffsets = Lists.newArrayList(100L, 200L, 300L);

        new Expectations() {
            {
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new Future<PProxyResult>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public PProxyResult get() {
                        PProxyResult proxyResult = new PProxyResult();
                        proxyResult.kafkaOffsetResult = new PKafkaOffsetProxyResult();
                        proxyResult.kafkaOffsetResult.partitionIds = partitions;
                        proxyResult.kafkaOffsetResult.latestOffsets = latestOffsets;
                        return proxyResult;
                    }

                    @Override
                    public PProxyResult get(long timeout, @NotNull TimeUnit unit) {
                        return get();
                    }
                };
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                List<Long> nodes = new ArrayList<>();
                nodes.add(1234L);
                return nodes;
            }
        };
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);
        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
            }
        };
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAlive() {
                return true;
            }
        };
        StatusPB status = new com.starrocks.proto.StatusPB();
        status.setStatusCode(0);
        PProxyResult proxyResult = new PProxyResult();
        proxyResult.setStatus(status);
        PKafkaMetaProxyResult kafkaOffsetProxyResult = new PKafkaMetaProxyResult();
        kafkaOffsetProxyResult.setPartitionIds(partitions);
        proxyResult.setKafkaMetaResult(kafkaOffsetProxyResult);
        PKafkaOffsetProxyResult kafkaOffsetResult = new PKafkaOffsetProxyResult();
        kafkaOffsetResult.setPartitionIds(partitions);
        kafkaOffsetResult.setLatestOffsets(latestOffsets);
        proxyResult.setKafkaOffsetResult(kafkaOffsetResult);
        new Expectations() {
            {
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = CompletableFuture.completedFuture(proxyResult);
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        Map<Integer, Long> offsets = api.getOffsets("brokerList", "topic", ImmutableMap.of(), partitions,
                true, WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(3, offsets.size());
        Assertions.assertEquals(Long.valueOf(100L), offsets.get(0));
        Assertions.assertEquals(Long.valueOf(200L), offsets.get(1));
        Assertions.assertEquals(Long.valueOf(300L), offsets.get(2));
    }

    @Test
    public void getOffsets_returnsCorrectOffsetsForBeginning() throws StarRocksException, RpcException {
        List<Integer> partitions = Lists.newArrayList(0, 1, 2);
        List<Long> beginningOffsets = Lists.newArrayList(0L, 10L, 20L);

        new Expectations() {
            {
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new Future<PProxyResult>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public PProxyResult get() {
                        PProxyResult proxyResult = new PProxyResult();
                        proxyResult.kafkaOffsetResult = new PKafkaOffsetProxyResult();
                        proxyResult.kafkaOffsetResult.partitionIds = partitions;
                        proxyResult.kafkaOffsetResult.beginningOffsets = beginningOffsets;
                        return proxyResult;
                    }

                    @Override
                    public PProxyResult get(long timeout, @NotNull TimeUnit unit) {
                        return get();
                    }
                };
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                List<Long> nodes = new ArrayList<>();
                nodes.add(1234L);
                return nodes;
            }
        };
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);
        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
            }
        };
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAlive() {
                return true;
            }
        };
        StatusPB status = new com.starrocks.proto.StatusPB();
        status.setStatusCode(0);
        PProxyResult proxyResult = new PProxyResult();
        proxyResult.setStatus(status);
        PKafkaMetaProxyResult kafkaOffsetProxyResult = new PKafkaMetaProxyResult();
        kafkaOffsetProxyResult.setPartitionIds(partitions);
        proxyResult.setKafkaMetaResult(kafkaOffsetProxyResult);
        PKafkaOffsetProxyResult kafkaOffsetResult = new PKafkaOffsetProxyResult();
        kafkaOffsetResult.setPartitionIds(partitions);
        kafkaOffsetResult.setBeginningOffsets(beginningOffsets);
        proxyResult.setKafkaOffsetResult(kafkaOffsetResult);
        new Expectations() {
            {
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = CompletableFuture.completedFuture(proxyResult);
            }
        };
        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        Map<Integer, Long> offsets = api.getOffsets("brokerList", "topic", ImmutableMap.of(),
                partitions, false, WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(3, offsets.size());
        Assertions.assertEquals(Long.valueOf(0L), offsets.get(0));
        Assertions.assertEquals(Long.valueOf(10L), offsets.get(1));
        Assertions.assertEquals(Long.valueOf(20L), offsets.get(2));
    }

    public void getOffsets_throwsExceptionWhenProxyRequestFails() throws StarRocksException, RpcException {
        new Expectations() {
            {
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = new RpcException("rpc failed");
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                List<Long> nodes = new ArrayList<>();
                nodes.add(1234L);
                return nodes;
            }
        };
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);
        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
            }
        };
        new MockUp<ComputeNode>() {
            @Mock
            public boolean isAlive() {
                return true;
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        Assertions.assertThrows(LoadException.class, () -> {
            api.getOffsets("brokerList", "topic", ImmutableMap.of(), Lists.newArrayList(0, 1, 2),
                    true, WarehouseManager.DEFAULT_RESOURCE);
        });
    }
}
