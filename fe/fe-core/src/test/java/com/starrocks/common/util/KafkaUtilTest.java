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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
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
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
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
    WarehouseManager warehouseManager;
    @Mocked
    Warehouse warehouse;
    @Mocked
    BackendServiceClient client;

    @Before
    public void before() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;
                BackendServiceClient.getInstance();
                minTimes = 0;
                result = client;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                return Lists.newArrayList(1L);
            }

            @Mock
            public Long getComputeNodeId(String warehouseName, LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public ComputeNode getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
                return new ComputeNode(1L, "127.0.0.1", 9030);
            }

            @Mock
            public ComputeNode getAllComputeNodeIdsAssignToTablet(String warehouseName, LakeTablet tablet) {
                return null;
            }

            @Mock
            public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse(long warehouseId) {
                return ImmutableMap.of(1L, new ComputeNode(1L, "127.0.0.1", 9030));
            }
        };
    }

    @Test
    public void testNoAliveComputeNode() throws UserException {
        new Expectations() {
            {
                service.getBackendOrComputeNode(anyLong);
                result = null;
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assert.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assert.assertEquals(
                "Failed to send get kafka partition info request. err: No alive backends or compute nodes", e.getMessage());
    }

    @Test
    public void testGetInfoRpcException() throws UserException, RpcException {
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
        LoadException e = Assert.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assert.assertTrue(e.getMessage().contains("err: rpc failed"));
    }

    @Test
    public void testGetInfoInterruptedException() throws UserException, RpcException {
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
        LoadException e = Assert.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assert.assertTrue(e.getMessage().contains("Got interrupted exception"));
    }

    @Test
    public void testGetInfoValidateObjectException() throws UserException, RpcException {
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
        LoadException e = Assert.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assert.assertTrue(e.getMessage().contains("err: BE is not alive"));
    }

    @Test
    public void testGetInfoFailed() throws UserException, RpcException {
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
        LoadException e = Assert.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assert.assertTrue(e.getMessage().contains("be process failed"));
    }

    @Test
    public void testWarehouseNotExist() throws UserException {
        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(long warehouseId) {
                throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("id: %d", 1L));
            }
        };

        KafkaUtil.ProxyAPI api = new KafkaUtil.ProxyAPI();
        LoadException e = Assert.assertThrows(LoadException.class, () -> api.getBatchOffsets(null));
        Assert.assertEquals("Failed to send get kafka partition info request. err: Warehouse id: 1 not exist.", e.getMessage());
    }
}
