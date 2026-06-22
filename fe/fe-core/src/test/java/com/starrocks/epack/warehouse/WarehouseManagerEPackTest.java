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

package com.starrocks.epack.warehouse;

import com.google.common.collect.Maps;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.extension.ExtensionManager;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogDeserializer;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;

public class WarehouseManagerEPackTest {

    @Mocked
    private EditLog editLog;

    @Mocked
    BaseSlotManager slotManager;

    @Before
    public void before() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        ExtensionManager.getInstance().loadExtensionsFromClassPath("target/classes");
    }

    @Test
    public void testWarehouseException() {
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        Map<Long, Warehouse> idToWh = Deencapsulation.getField(mgr, "idToWh");
        Map<String, Warehouse> nameToWh = Deencapsulation.getField(mgr, "nameToWh");
        LocalWarehouse wh1 = new LocalWarehouse(1L, "wh1", null, "");
        wh1.suspendSelf();
        idToWh.put(wh1.getId(), wh1);
        nameToWh.put(wh1.getName(), wh1);

        SuspendWarehouseStmt suspendStmt = new SuspendWarehouseStmt("wh1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh1 has been suspended.",
                () -> mgr.suspendWarehouse(suspendStmt));

        // warehouse exist
        CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh1", Maps.newHashMap(), "");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh1 already exists.",
                () -> mgr.createWarehouse(createStmt));

        // warehouse not exist
        DropWarehouseStmt dropStmt = new DropWarehouseStmt(false, "wh2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh2 not exist.",
                () -> mgr.dropWarehouse(dropStmt));

        // warehouse used by compaction
        Config.lake_compaction_warehouse = "wh1";
        DropWarehouseStmt dropStmt2 = new DropWarehouseStmt(false, "wh1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "warehouse wh1 is used by compaction or background job," +
                " adjust lake_compaction_warehouse or lake_background_warehouse first",
                () -> mgr.dropWarehouse(dropStmt2));
        Config.lake_compaction_warehouse = "default_warehouse";
    }

    byte[] writeEditLog(short op, Writable w) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream stream = new DataOutputStream(byteOut);
        stream.writeShort(op);
        Text.writeString(stream, GsonUtils.GSON.toJson(w, w.getClass()));
        return byteOut.toByteArray();
    }

    @Test
    public void testCreateWarehouse() {
        long warehouseId = 345;
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public long createWorkerGroup(String size, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel, int warmupTimeoutSecs,
                                          Map<String, String> properties) throws DdlException {
                return warehouseId;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };

        { // the warehouse is created with default properties
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh1", Maps.newHashMap(), "");
            ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));

            Warehouse warehouse = mgr.getWarehouse("wh1");
            Assert.assertNotNull(warehouse);
            Assert.assertTrue(warehouse instanceof LocalWarehouse);
            WarehouseProperty property = ((LocalWarehouse) warehouse).getProperty();
            Assert.assertEquals(new WarehouseProperty(), property);
        }

        { // the warehouse is created with customized properties
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            HashMap<String, String> props = Maps.newHashMap();
            props.put("compute_replica", "2");
            props.put("replication_type", "async");
            props.put("warmup_level", "all");
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh2", props, "");
            ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));

            Warehouse warehouse = mgr.getWarehouse("wh2");
            Assert.assertNotNull(warehouse);
            Assert.assertTrue(warehouse instanceof LocalWarehouse);
            WarehouseProperty property = ((LocalWarehouse) warehouse).getProperty();
            WarehouseProperty expected = new WarehouseProperty(2, WarehouseProperty.ReplicationType.ASYNC,
                    WarehouseProperty.WarmupLevelType.ALL, false);
            Assert.assertEquals(expected, property);
        }

        { // the warehouse is created with a per-warehouse warmup timeout override
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            HashMap<String, String> props = Maps.newHashMap();
            props.put("warmup_timeout_secs", "600");
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh3", props, "");
            ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));

            WarehouseProperty property = ((LocalWarehouse) mgr.getWarehouse("wh3")).getProperty();
            Assert.assertEquals(600, property.getWarmupTimeoutSecs());
        }

        { // a negative warmup timeout is rejected
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            HashMap<String, String> props = Maps.newHashMap();
            props.put("warmup_timeout_secs", "-1");
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh4", props, "");
            DdlException exception = Assert.assertThrows(DdlException.class, () -> mgr.createWarehouse(createStmt));
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("warmup timeout"));
        }

        { // Unknown/Unsupported properties
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            HashMap<String, String> props = Maps.newHashMap();
            props.put("compute_replica", "2");
            props.put("replication_type", "async");
            props.put("ReplicationType", "async");
            props.put("warmup_level", "all");
            props.put("replication_num", "1");
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh2", props, "");
            DdlException exception = Assert.assertThrows(DdlException.class, () -> mgr.createWarehouse(createStmt));
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("ReplicationType"));
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("replication_num"));
        }
    }

    @Test
    public void testAlterWarehouse() throws DdlException {
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public void updateWorkerGroup(long workerGroupId, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel, OptionalInt warmupTimeoutSecs) throws DdlException {
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };
        {
            Map<String, String> m = new HashMap<>();
            m.put("compute_replica", "3");
            AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.initDefaultWarehouse();
            mgr.alterWarehouse(alterStmt);
        }
        {
            Map<String, String> m = new HashMap<>();
            m.put("replication_type", "ASYNC");
            AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.initDefaultWarehouse();
            mgr.alterWarehouse(alterStmt);
        }
        {
            Map<String, String> m = new HashMap<>();
            m.put("warmup_level", "ALL");
            AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.initDefaultWarehouse();
            mgr.alterWarehouse(alterStmt);
        }
        {
            Map<String, String> m = new HashMap<>();
            m.put("warmup_level", "Meta");
            m.put("replication_type", "aSYNC");
            AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.initDefaultWarehouse();
            mgr.alterWarehouse(alterStmt);
            Warehouse warehouse = mgr.getWarehouse("default_warehouse");
            Assert.assertTrue(warehouse instanceof  LocalWarehouse);
            WarehouseProperty property = ((LocalWarehouse) warehouse).getProperty();
            Assert.assertEquals(WarehouseProperty.ReplicationType.ASYNC, property.getReplicationType());
            Assert.assertEquals(WarehouseProperty.WarmupLevelType.META, property.getWarmupLevel());
        }
        {
            Map<String, String> m = new HashMap<>();
            m.put("warmup_timeout_secs", "900");
            AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.initDefaultWarehouse();
            mgr.alterWarehouse(alterStmt);
            Warehouse warehouse = mgr.getWarehouse("default_warehouse");
            Assert.assertTrue(warehouse instanceof LocalWarehouse);
            WarehouseProperty property = ((LocalWarehouse) warehouse).getProperty();
            Assert.assertEquals(900, property.getWarmupTimeoutSecs());
        }

        {
            Map<String, String> m = new HashMap<>();
            // incorrect one
            m.put("_warmup_", "Meta");
            m.put("+replication+", "aSYNC");
            // correct one
            m.put("warmup_level", "Meta");
            m.put("replication_type", "aSYNC");
            AlterWarehouseStmt alterStmt = new AlterWarehouseStmt("default_warehouse", m);
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.initDefaultWarehouse();

            DdlException exception = Assert.assertThrows(DdlException.class, () -> mgr.alterWarehouse(alterStmt));
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("_warmup_"));
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("+replication+"));
        }
    }

    @Test
    public void testCreateWarehouseAndReplayCreateWarehouse() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };

        long workerGroupId = GlobalStateMgr.getCurrentState().getNextId();
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public long createWorkerGroup(String size, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel, int warmupTimeoutSecs,
                                          Map<String, String> properties) throws DdlException {
                return workerGroupId;
            }
        };

        { // the warehouse is created with default properties, the editLog is captured
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh1", Maps.newHashMap(), "");
            ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));

            Warehouse warehouse = mgr.getWarehouse("wh1");
            Assert.assertNotNull(warehouse);
            Assert.assertTrue(warehouse instanceof LocalWarehouse);
            LocalWarehouse localWarehouse = (LocalWarehouse) warehouse;
            WarehouseProperty property = localWarehouse.getProperty();
            Assert.assertEquals(new WarehouseProperty(), property);
            Assert.assertEquals(1L, localWarehouse.getClusters().size());
            // verify the cngroup
            Cluster cluster = localWarehouse.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
            Assert.assertNotNull(cluster);
            Assert.assertEquals(workerGroupId, cluster.getWorkerGroupId());
        }

        AtomicReference<byte[]> bytes = new AtomicReference<>();
        new Verifications() {
            {
                short op;
                Writable w;
                editLog.logJsonObject(op = withCapture(), w = withCapture());
                Assert.assertNotNull(w);
                bytes.set(writeEditLog(op, w));
            }
        };

        { // replay the editLog to recreate the warehouse
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.get()));
            short opCode = in.readShort();
            Writable writable = EditLogDeserializer.deserialize(opCode, in);
            Assert.assertEquals(OperationType.OP_CREATE_WAREHOUSE, opCode);
            Assert.assertTrue(writable instanceof Warehouse);

            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            mgr.replayCreateWarehouse((Warehouse) writable);

            // verify the replayed result
            Warehouse warehouse = mgr.getWarehouse("wh1");
            Assert.assertNotNull(warehouse);
            Assert.assertTrue(warehouse instanceof LocalWarehouse);
            LocalWarehouse localWarehouse = (LocalWarehouse) warehouse;
            WarehouseProperty property = localWarehouse.getProperty();
            Assert.assertEquals(new WarehouseProperty(), property);
            Assert.assertEquals(1L, localWarehouse.getClusters().size());
            // verify the cngroup
            Cluster cluster = localWarehouse.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
            Assert.assertNotNull(cluster);
            Assert.assertEquals(workerGroupId, cluster.getWorkerGroupId());
        }
    }

    @Test
    public void testRecordWarehouseInfo() throws DdlException {
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        CNGroupResource resource1 = CNGroupResource.of(1, 10);
        mgr.recordWarehouseInfoForTable(100 /* tableId */, resource1);
        Assert.assertEquals(mgr.getLastTransactionWarehouseInfoForTable(100), CNGroupResource.of(1, 10));
        CNGroupResource resource2 = CNGroupResource.of(11, 100);
        mgr.recordWarehouseInfoForTable(100 /* tableId */, resource2);
        Assert.assertEquals(mgr.getLastTransactionWarehouseInfoForTable(100), CNGroupResource.of(11, 100));
        mgr.removeTableWarehouseInfo(100 /* tableId */);
        Assert.assertEquals(mgr.getLastTransactionWarehouseInfoForTable(100), CNGroupResource.of(0, 0));
        CNGroupResource resource3 = CNGroupResource.of(111, 1000);
        mgr.recordWarehouseInfoForTable(100 /* tableId */, resource3);
        Assert.assertEquals(mgr.getLastTransactionWarehouseInfoForTable(100), CNGroupResource.of(111, 1000));
    }

    @Test
    public void testGetBackgroundComputeResource() throws DdlException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };
        long workerGroupId = GlobalStateMgr.getCurrentState().getNextId();
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public long createWorkerGroup(String size, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel, int warmupTimeoutSecs,
                                          Map<String, String> properties) throws DdlException {
                return workerGroupId;
            }
        };
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        mgr.initDefaultWarehouse();

        CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "wh1", Maps.newHashMap(), "");
        ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));
        Warehouse warehouse = mgr.getWarehouse("wh1");
        CNGroupResource resource = CNGroupResource.of(warehouse.getId(), 10);
        mgr.recordWarehouseInfoForTable(100 /* tableId */, resource);
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                if (acquireContext.getWarehouseId() == 0) {
                    return CNGroupResource.of(0, 0);
                }
                return acquireContext.getPrevComputeResource();
            }
        };
        ComputeResource r1 = mgr.getBackgroundComputeResource(100);
        Assert.assertEquals(r1.getWarehouseId(), warehouse.getId());
        Assert.assertEquals(r1.getWorkerGroupId(), 10);
        mgr.removeTableWarehouseInfo(100 /* tableId */);
        ComputeResource r2 = mgr.getBackgroundComputeResource(100);
        Assert.assertEquals(r2.getWarehouseId(), 0);
        Assert.assertEquals(r2.getWorkerGroupId(), 0);
    }

    @Test
    public void testGetVectorIndexBuildComputeResource() throws DdlException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };
        long workerGroupId = GlobalStateMgr.getCurrentState().getNextId();
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public long createWorkerGroup(String size, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel, int warmupTimeoutSecs,
                                          Map<String, String> properties) throws DdlException {
                return workerGroupId;
            }
        };
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        mgr.initDefaultWarehouse();

        CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, "vi_wh", Maps.newHashMap(), "");
        ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));
        Warehouse warehouse = mgr.getWarehouse("vi_wh");

        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return CNGroupResource.of(acquireContext.getWarehouseId(), 10);
            }
        };

        // Cold path: no prior transaction warehouse — uses Config.lake_vector_index_build_warehouse.
        String prevConfig = Config.lake_vector_index_build_warehouse;
        try {
            Config.lake_vector_index_build_warehouse = "vi_wh";
            ComputeResource cold = mgr.getVectorIndexBuildComputeResource(200);
            Assert.assertEquals(warehouse.getId(), cold.getWarehouseId());

            // Warm path: prior transaction warehouse recorded for table — reuses it.
            CNGroupResource resource = CNGroupResource.of(warehouse.getId(), 77);
            mgr.recordWarehouseInfoForTable(200, resource);
            ComputeResource warm = mgr.getVectorIndexBuildComputeResource(200);
            Assert.assertEquals(warehouse.getId(), warm.getWarehouseId());
        } finally {
            Config.lake_vector_index_build_warehouse = prevConfig;
        }
    }

    @Test
    public void testSuspendWarehouseAndReplay() throws IOException, DdlException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };
        long workerGroupId = GlobalStateMgr.getCurrentState().getNextId();
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public long createWorkerGroup(String size, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel, int warmupTimeoutSecs,
                                          Map<String, String> properties) throws DdlException {
                return workerGroupId;
            }
        };

        List<Pair<Short, String>> logs = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObject(short op, Object w) {
                try {
                    String jsonStr = GsonUtils.GSON.toJson(w, w.getClass());
                    logs.add(Pair.of(op, jsonStr));
                } catch (Exception e) {
                    Assertions.fail("cannot write json");
                }
            }
        };


        String warehouseName = "wh1";
        long resumeTime = 0;
        {
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            CreateWarehouseStmt createStmt = new CreateWarehouseStmt(false, warehouseName, Maps.newHashMap(), "");
            ExceptionChecker.expectThrowsNoException(() -> mgr.createWarehouse(createStmt));

            Warehouse warehouse = mgr.getWarehouse(warehouseName);
            Assertions.assertNotNull(warehouse);
            Assertions.assertInstanceOf(LocalWarehouse.class, warehouse);
            LocalWarehouse localWarehouse = (LocalWarehouse) warehouse;
            Assertions.assertEquals(LocalWarehouse.WarehouseState.AVAILABLE, localWarehouse.getState());

            SuspendWarehouseStmt suspendStat = new SuspendWarehouseStmt(warehouseName);
            ExceptionChecker.expectThrowsNoException(() -> mgr.suspendWarehouse(suspendStat));

            Assertions.assertEquals(LocalWarehouse.WarehouseState.SUSPENDED, localWarehouse.getState());
            resumeTime = localWarehouse.getResumeTime();
        }

        // should be two logs, one for create, one for suspend
        Assertions.assertEquals(2L, logs.size());

        { // replay the editLog to suspend the warehouse
            WarehouseManagerEPack mgr = new WarehouseManagerEPack();
            Assertions.assertNull(mgr.getWarehouseAllowNull(warehouseName));

            // replay edit log
            for (Pair<Short, String> log : logs) {
                short opCode = log.getLeft();
                String jsonStr = log.getRight();
                LocalWarehouse replayWh = GsonUtils.GSON.fromJson(jsonStr, LocalWarehouse.class);

                if (opCode == OperationType.OP_CREATE_WAREHOUSE) {
                    mgr.replayCreateWarehouse(replayWh);
                } else if (opCode == OperationType.OP_ALTER_WAREHOUSE) {
                    mgr.replayAlterWarehouse(replayWh);
                } else {
                    Assert.fail("unexpected op code: " + opCode);
                }
            }

            // verify the replayed result
            Warehouse warehouse = mgr.getWarehouse(warehouseName);
            Assertions.assertNotNull(warehouse);
            Assertions.assertInstanceOf(LocalWarehouse.class, warehouse);
            LocalWarehouse localWarehouse = (LocalWarehouse) warehouse;
            Assertions.assertEquals(LocalWarehouse.WarehouseState.SUSPENDED, localWarehouse.getState());
            Assertions.assertEquals(resumeTime, localWarehouse.getResumeTime());
        }
    }
}
