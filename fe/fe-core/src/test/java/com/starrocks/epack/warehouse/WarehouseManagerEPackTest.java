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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class WarehouseManagerEPackTest {

    @Mocked
    private EditLog editLog;

    @Before
    public void before() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
    }

    @Test
    public void testWarehouseException() {
        WarehouseManagerEPack mgr = new WarehouseManagerEPack();
        Map<Long, Warehouse> idToWh = Deencapsulation.getField(mgr, "idToWh");
        Map<String, Warehouse> nameToWh = Deencapsulation.getField(mgr, "nameToWh");
        LocalWarehouse wh1 = new LocalWarehouse(1L, "wh1", 1L, null, "");
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

    @Test
    public void testCreateWarehouse() {
        long warehouseId = 345;
        new MockUp<StarOSAgentEpack>() {
            @Mock
            public long createWorkerGroup(String size, int replicaNumber, ReplicationType replicationType,
                                          WarmupLevel warmupLevel) throws DdlException {
                return warehouseId;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
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
                                          WarmupLevel warmupLevel) throws DdlException {
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
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
}
