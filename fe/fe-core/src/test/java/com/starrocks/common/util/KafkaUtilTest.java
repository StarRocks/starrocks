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

import com.google.common.collect.Lists;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.PKafkaOffsetBatchProxyResult;
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
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.MockedWarehouseManager;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    @Mocked
    WarehouseManager warehouseManager;

    @BeforeEach
    public void before() throws StarRocksException {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        new Expectations() {
            {
                BackendServiceClient.getInstance();
                minTimes = 0;
                result = client;
                warehouseManager.getAllComputeNodeIds(anyLong);
                minTimes = 0;
                result = Lists.newArrayList(1L, 2L);
            }
        };
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
                "Failed to send get kafka partition info request. err: No alive backends or compute nodes", e.getMessage());
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

        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = createFailProxyResultFuture();
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
        Assertions.assertEquals("Failed to send get kafka partition info request. err: Warehouse id: 1 not exist.",
                e.getMessage());
    }

    private Future<PProxyResult> createSuccessProxyResultFuture() {
        PProxyResult proxyResult = new PProxyResult();
        StatusPB status = new StatusPB();
        // success
        status.statusCode = 0;
        status.errorMsgs = Lists.newArrayList();
        proxyResult.status = status;

        PKafkaOffsetProxyResult offsetResult = new PKafkaOffsetProxyResult();
        offsetResult.partitionIds = Lists.newArrayList(0);
        offsetResult.beginningOffsets = Lists.newArrayList(200L);
        offsetResult.latestOffsets = Lists.newArrayList(300L);
        PKafkaOffsetBatchProxyResult offsetBatchResult = new PKafkaOffsetBatchProxyResult();
        offsetBatchResult.results = Lists.newArrayList(offsetResult);
        proxyResult.kafkaOffsetBatchResult = offsetBatchResult;

        return new Future<PProxyResult>() {
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

    private Future<PProxyResult> createFailProxyResultFuture() {
        PProxyResult proxyResult = new PProxyResult();
        StatusPB status = new StatusPB();
        // cancelled
        status.statusCode = 1;
        status.errorMsgs = Lists.newArrayList("be process failed");
        proxyResult.status = status;

        return new Future<PProxyResult>() {
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

    @Test
    public void testGetInfoSuccess() throws StarRocksException, RpcException {
        Backend backend = new Backend(1L, "127.0.0.1", 9050);
        backend.setBeRpcPort(8060);
        backend.setAlive(true);

        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = backend;
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                result = createSuccessProxyResultFuture();
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        List<PKafkaOffsetProxyResult> results = api.getBatchOffsets(null);
        Assertions.assertEquals(1, results.size());
        PKafkaOffsetProxyResult result = results.get(0);
        Assertions.assertEquals(1, result.partitionIds.size());
        Assertions.assertEquals(0, result.partitionIds.get(0).intValue());
        Assertions.assertEquals(1, result.beginningOffsets.size());
        Assertions.assertEquals(200L, result.beginningOffsets.get(0).longValue());
        Assertions.assertEquals(1, result.latestOffsets.size());
        Assertions.assertEquals(300L, result.latestOffsets.get(0).longValue());
    }

    @Test
    public void testGetInfoRetry() throws StarRocksException, RpcException {
        Backend backend1 = new Backend(1L, "127.0.0.1", 9050);
        backend1.setBeRpcPort(8060);
        backend1.setAlive(true);

        Backend backend2 = new Backend(2L, "127.0.0.2", 9050);
        backend2.setBeRpcPort(8060);
        backend2.setAlive(true);

        new Expectations() {
            {
                service.getBackendOrComputeNode(1L);
                result = backend1;
                service.getBackendOrComputeNode(2L);
                result = backend2;
                client.getInfo((TNetworkAddress) any, (PProxyRequest) any);
                returns(createFailProxyResultFuture(), createSuccessProxyResultFuture());
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        List<PKafkaOffsetProxyResult> results = api.getBatchOffsets(null);
        Assertions.assertEquals(1, results.size());
        PKafkaOffsetProxyResult result = results.get(0);
        Assertions.assertEquals(1, result.partitionIds.size());
        Assertions.assertEquals(0, result.partitionIds.get(0).intValue());
        Assertions.assertEquals(1, result.beginningOffsets.size());
        Assertions.assertEquals(200L, result.beginningOffsets.get(0).longValue());
        Assertions.assertEquals(1, result.latestOffsets.size());
        Assertions.assertEquals(300L, result.latestOffsets.get(0).longValue());
    }
}
