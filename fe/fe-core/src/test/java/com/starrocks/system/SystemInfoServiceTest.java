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

package com.starrocks.system;

import com.google.api.client.util.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.Pair;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.UpdateHistoricalNodeLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SystemInfoServiceTest {

    SystemInfoService service;

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    EditLog editLog;

    @BeforeEach
    public void setUp() throws NoSuchFieldException,
            SecurityException,
            IllegalArgumentException,
            IllegalAccessException {
        service = new SystemInfoService();
        Field field = FrontendOptions.class.getDeclaredField("useFqdn");
        field.setAccessible(true);
        field.set(null, true);
    }

    private void mockFunc() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                result = editLog;
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logBackendStateChange(Backend be) {
            }
        };
    }

    @Test
    public void testUpdateBackendHostWithOneBe() throws Exception {
        mockFunc();
        Backend be = new Backend(100, "127.0.0.1", 1000);
        service.addBackend(be);
        ModifyBackendClause clause = new ModifyBackendClause("127.0.0.1", "sandbox");
        service.modifyBackendHost(clause);
        Backend backend = service.getBackendWithHeartbeatPort("sandbox", 1000);
        Assertions.assertNotNull(backend);
    }

    @Test
    public void testUpdateBackendHostWithMoreBe() throws Exception {
        mockFunc();
        Backend be1 = new Backend(100, "127.0.0.1", 1000);
        Backend be2 = new Backend(101, "127.0.0.1", 1001);
        service.addBackend(be1);
        service.addBackend(be2);
        ModifyBackendClause clause = new ModifyBackendClause("127.0.0.1", "sandbox");
        service.modifyBackendHost(clause);
        Backend backend = service.getBackendWithHeartbeatPort("sandbox", 1000);
        Assertions.assertNotNull(backend);
    }

    @Test
    public void testUpdateBackendAddressNotFoundBe() {
        assertThrows(DdlException.class, () -> {
            Backend be = new Backend(100, "originalHost", 1000);
            service.addBackend(be);
            ModifyBackendClause clause = new ModifyBackendClause("originalHost-test", "sandbox");
            // This case will occur backend [%s] not found exception
            service.modifyBackendHost(clause);
        });
    }

    /**
     * Test method for {@link SystemInfoService#modifyBackendProperty(ModifyBackendClause)}.
     */
    @Test
    public void testModifyBackendProperty() throws DdlException {
        Backend be = new Backend(100, "originalHost", 1000);
        service.addBackend(be);
        Map<String, String> properties = Maps.newHashMap();
        String location = "rack:rack1";
        properties.put(AlterSystemStmtAnalyzer.PROP_KEY_LOCATION, location);
        ModifyBackendClause clause = new ModifyBackendClause("originalHost:1000", properties);
        service.modifyBackendProperty(clause);
        Backend backend = service.getBackendWithHeartbeatPort("originalHost", 1000);
        Assertions.assertNotNull(backend);
        Assertions.assertEquals("{rack=rack1}", backend.getLocation().toString());
    }

    @Test
    public void testUpdateBackend() throws Exception {
        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);
        service.updateInMemoryStateBackend(be);
        Backend newBe = service.getBackend(10001);
        Assertions.assertTrue(newBe.getHost().equals("newHost"));
    }

    @Test
    public void testGetBackendOrComputeNode() {
        mockNet();

        Backend be = new Backend(10001, "host1", 1000);
        service.addBackend(be);
        ComputeNode cn = new ComputeNode(10002, "host2", 1000);
        cn.setBePort(1001);
        service.addComputeNode(cn);

        Assertions.assertEquals(be, service.getBackendOrComputeNode(be.getId()));
        Assertions.assertEquals(cn, service.getBackendOrComputeNode(cn.getId()));
        Assertions.assertNull(service.getBackendOrComputeNode(/* Not Exist */ 100));

        Assertions.assertEquals(cn, service.getBackendOrComputeNodeWithBePort("host2", 1001));
        Assertions.assertFalse(service.checkNodeAvailable(cn));
        Assertions.assertFalse(service.checkNodeAvailable(be));

        List<ComputeNode> nodes = service.backendAndComputeNodeStream().collect(Collectors.toList());
        Assertions.assertEquals(2, nodes.size());
        Assertions.assertEquals(be, nodes.get(0));
        Assertions.assertEquals(cn, nodes.get(1));
    }

    @Test
    public void testDropBackend() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new Expectations() {
            {
                service.getBackendWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = be;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addBackend(be);
        be.setStarletPort(1001);
        service.dropBackend("newHost", 1000, WarehouseManager.DEFAULT_WAREHOUSE_NAME, "", false);
        Backend beIP = service.getBackendWithHeartbeatPort("newHost", 1000);
        Assertions.assertTrue(beIP == null);

        Config.enable_trace_historical_node = savedConfig;
    }

    @Test
    public void testDropComputeNode() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        ComputeNode cn = new ComputeNode(10001, "newHost", 1000);
        service.addComputeNode(cn);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new Expectations() {
            {
                service.getComputeNodeWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = cn;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addComputeNode(cn);
        cn.setStarletPort(1001);
        service.dropComputeNode("newHost", 1000, WarehouseManager.DEFAULT_WAREHOUSE_NAME, "");
        ComputeNode cnIP = service.getComputeNodeWithHeartbeatPort("newHost", 1000);
        Assertions.assertTrue(cnIP == null);

        Config.enable_trace_historical_node = savedConfig;
    }
    @Test
    public void testDropComputeNode2() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        ComputeNode cn = new ComputeNode(10001, "newHost", 1000);
        service.addComputeNode(cn);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addComputeNode(cn);
        cn.setStarletPort(1001);

        {
            Assertions.assertThrows(DdlException.class, () ->
                    service.dropComputeNode("newHost", 1000, "warehousename-cn-not-exists", ""),
                    ErrorCode.ERR_UNKNOWN_WAREHOUSE.formatErrorMsg("name: warehousename-cn-not-exists"));
        }

        service.dropComputeNode("newHost", 1000, "", "");
        ComputeNode cnIP = service.getComputeNodeWithHeartbeatPort("newHost", 1000);
        Assertions.assertNull(cnIP);


        Config.enable_trace_historical_node = savedConfig;
    }

    @Test
    public void testDropBackendWithoutWarehouse() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return localMetastore;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addBackend(be);
        be.setStarletPort(1001);
        service.dropBackend("newHost", 1000, null, null, false);
        Backend beIP = service.getBackendWithHeartbeatPort("newHost", 1000);
        Assertions.assertNull(beIP);

        Config.enable_trace_historical_node = savedConfig;
    }

    @Test
    public void testDropComputeNodeWithoutWarehouse() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        ComputeNode cn = new ComputeNode(10001, "newHost", 1000);
        service.addComputeNode(cn);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return localMetastore;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addComputeNode(cn);
        cn.setStarletPort(1001);
        service.dropComputeNode("newHost", 1000, null, null);
        ComputeNode cnIP = service.getComputeNodeWithHeartbeatPort("newHost", 1000);
        Assertions.assertNull(cnIP);

        Config.enable_trace_historical_node = savedConfig;
    }

    @Test
    public void testDropBackendWithInvalidWarehouse() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return localMetastore;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addBackend(be);
        be.setStarletPort(1001);
        Assertions.assertThrows(DdlException.class,
                () -> service.dropBackend("newHost", 1000, "not_existed_warehouse", null, false));

        Config.enable_trace_historical_node = savedConfig;
    }

    @Test
    public void testDropComputeNodeWithInvalidWarehouse() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Boolean savedConfig = Config.enable_trace_historical_node;
        Config.enable_trace_historical_node = true;

        ComputeNode cn = new ComputeNode(10001, "newHost", 1000);
        service.addComputeNode(cn);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public LocalMetastore getLocalMetastore() {
                return localMetastore;
            }

            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return WarehouseManager.DEFAULT_RESOURCE;
            }
        };
        service.addComputeNode(cn);
        cn.setStarletPort(1001);
        Assertions.assertThrows(DdlException.class,
                () -> service.dropComputeNode("newHost", 1000, "not_existed_warehouse", null));

        Config.enable_trace_historical_node = savedConfig;
    }

    @Test
    public void testReplayUpdateHistoricalNode() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        HistoricalNodeMgr historicalNodeMgr = new HistoricalNodeMgr();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getHistoricalNodeMgr();
                result = historicalNodeMgr;
            }
        };

        List<Long> backendIds = Arrays.asList(101L, 102L);
        List<Long> computeNodeIds = Arrays.asList(201L, 202L, 203L);
        long updateTime = System.currentTimeMillis();
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        long workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
        UpdateHistoricalNodeLog log = new UpdateHistoricalNodeLog(warehouseId, workerGroupId, updateTime, backendIds,
                computeNodeIds);

        service.replayUpdateHistoricalNode(log);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalBackendIds(warehouseId, workerGroupId).size(), 2);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouseId, workerGroupId).size(), 3);
    }

    @Test
    public void testReplayOldFormatOfUpdateHistoricalNode() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<UpdateHistoricalNodeLog>() {
            @Mock
            public String getWarehouse() {
                return WarehouseManager.DEFAULT_WAREHOUSE_NAME;
            }
        };

        HistoricalNodeMgr historicalNodeMgr = new HistoricalNodeMgr();
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getHistoricalNodeMgr();
                result = historicalNodeMgr;
            }
        };

        List<Long> backendIds = Arrays.asList(101L, 102L);
        List<Long> computeNodeIds = Arrays.asList(201L, 202L, 203L);
        long updateTime = System.currentTimeMillis();
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        long workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
        UpdateHistoricalNodeLog log = new UpdateHistoricalNodeLog(warehouseId, workerGroupId, updateTime, backendIds,
                computeNodeIds);

        service.replayUpdateHistoricalNode(log);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalBackendIds(warehouseId, workerGroupId).size(), 2);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouseId, workerGroupId).size(), 3);
    }

    @Test
    public void testReplayDropBackend() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        Backend be = new Backend(10001, "newHost", 1000);
        be.setStarletPort(1001);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);
        new Expectations() {
            {
                service.getBackendWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = be;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
            }
        };

        service.addBackend(be);
        service.replayDropBackend(be);
        Backend beIP = service.getBackendWithHeartbeatPort("newHost", 1000);
        Assertions.assertTrue(beIP == null);
    }

    @Mocked
    InetAddress addr;

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr;
            }
        };
        new Expectations() {
            {
                addr.getHostAddress();
                result = "127.0.0.1";
            }
        };
    }

    @Test
    public void testGetBackendWithBePort() throws Exception {

        mockNet();

        Backend be1 = new Backend(10001, "127.0.0.1", 1000);
        be1.setBePort(1001);
        service.addBackend(be1);
        Backend beIP1 = service.getBackendWithBePort("127.0.0.1", 1001);

        service.dropAllBackend();

        Backend be2 = new Backend(10001, "newHost-1", 1000);
        be2.setBePort(1001);
        service.addBackend(be2);
        Backend beFqdn = service.getBackendWithBePort("127.0.0.1", 1001);

        Assertions.assertTrue(beFqdn != null && beIP1 != null);

        service.dropAllBackend();

        Backend be3 = new Backend(10001, "127.0.0.1", 1000);
        be3.setBePort(1001);
        service.addBackend(be3);
        Backend beIP3 = service.getBackendWithBePort("127.0.0.2", 1001);
        Assertions.assertTrue(beIP3 == null);
    }

    @Test
    public void testGetBackendOnlyWithHost() throws Exception {

        Backend be = new Backend(10001, "newHost", 1000);
        be.setBePort(1001);
        service.addBackend(be);
        List<Backend> bes = service.getBackendOnlyWithHost("newHost");
        Assertions.assertTrue(bes.size() == 1);
    }

    @Test
    public void testGetBackendIdWithStarletPort() throws Exception {
        Backend be = new Backend(10001, "newHost", 1000);
        be.setStarletPort(10001);
        service.addBackend(be);
        long backendId = service.getBackendIdWithStarletPort("newHost", 10001);
        Assertions.assertEquals(be.getId(), backendId);
    }

    @Test
    public void testUpdateReportVersionIncreasing() throws Exception {
        long[] versions = new long[] {10, 5, 3, 2, 4, 1, 9, 7, 8, 6};
        AtomicLong version = new AtomicLong();
        version.set(0);

        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            final int index = i;
            new Thread(() -> {
                service.updateReportVersionIncrementally(version, versions[index]);
                System.out.println("updated version: " + versions[index]);
                latch.countDown();
            }).start();
        }

        latch.await();

        Assertions.assertEquals(10L, version.get());
    }

    @Test
    public void testGetHostAndPort() {
        String ipv4 = "192.168.1.2:9050";
        String ipv6 = "[fe80::5054:ff:fec9:dee0]:9050";
        String ipv6Error = "fe80::5054:ff:fec9:dee0:dee0";
        try {
            Pair<String, Integer> ipv4Addr = SystemInfoService.validateHostAndPort(ipv4, false);
            Assertions.assertEquals("192.168.1.2", ipv4Addr.first);
            Assertions.assertEquals(9050, ipv4Addr.second.intValue());
        } catch (SemanticException e) {
            e.printStackTrace();
            Assertions.fail();
        }
        try {
            Pair<String, Integer> ipv6Addr = SystemInfoService.validateHostAndPort(ipv6, false);
            Assertions.assertEquals("fe80::5054:ff:fec9:dee0", ipv6Addr.first);
            Assertions.assertEquals(9050, ipv6Addr.second.intValue());
        } catch (SemanticException e) {
            e.printStackTrace();
            Assertions.fail();
        }
        try {
            SystemInfoService.validateHostAndPort(ipv6Error, false);
            Assertions.fail();
        } catch (SemanticException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetComputeNodeWithBePort() throws Exception {
        mockNet();

        ComputeNode be1 = new ComputeNode(10001, "127.0.0.1", 1000);
        be1.setBePort(1001);
        service.addComputeNode(be1);
        ComputeNode beIP1 = service.getComputeNodeWithBePort("127.0.0.1", 1001);

        service.dropAllComputeNode();

        ComputeNode be2 = new ComputeNode(10001, "newHost-1", 1000);
        be2.setBePort(1001);
        service.addComputeNode(be2);
        ComputeNode beFqdn = service.getComputeNodeWithBePort("127.0.0.1", 1001);

        Assertions.assertTrue(beFqdn != null && beIP1 != null);

        service.dropAllComputeNode();

        ComputeNode be3 = new ComputeNode(10001, "127.0.0.1", 1000);
        be3.setBePort(1001);
        service.addComputeNode(be3);
        ComputeNode beIP3 = service.getComputeNodeWithBePort("127.0.0.2", 1001);
        Assertions.assertTrue(beIP3 == null);
    }

    @Test
    public void testUpdateBackendAddressInSharedDataMode() {
        assertThrows(DdlException.class, () -> {
            new MockUp<RunMode>() {
                @Mock
                public boolean isSharedDataMode() {
                    return true;
                }
            };
            Backend be = new Backend(100, "originalHost", 1000);
            service.addBackend(be);
            ModifyBackendClause clause = new ModifyBackendClause("originalHost-test", "sandbox");
            // throw not support exception
            service.modifyBackendHost(clause);
        });
    }

}
