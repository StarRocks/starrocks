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


package com.starrocks.server;

import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.epack.persist.DropWarehouseLog;
import com.starrocks.epack.server.WarehouseManagerEpack;
import com.starrocks.epack.sql.ast.CreateWarehouseStmt;
import com.starrocks.epack.sql.ast.DropWarehouseStmt;
import com.starrocks.epack.sql.ast.ResumeWarehouseStmt;
import com.starrocks.epack.sql.ast.SuspendWarehouseStmt;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;


public class WarehouseMgrTest {
    private String fileName = "./testWarehouseMgr";

    @Before
    public void setUp() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
    }

    @After
    public void tearDownCreate() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testReplay(@Mocked StarOSAgentEpack starOSAgent) throws Exception {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }
        };

        new Expectations() {
            {
                starOSAgent.deleteWorkerGroup(anyLong);
                result = null;
                minTimes = 0;

                starOSAgent.createWorkerGroup(anyString);
                result = -1L;
                minTimes = 0;
            }
        };

        WarehouseManagerEpack warehouseMgr = (WarehouseManagerEpack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = new LocalWarehouse(10000, "warehouse_1", 1000, null);
        warehouse.initCluster();
        warehouseMgr.replayCreateWarehouse(warehouse);
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));
        Assert.assertEquals(Warehouse.WarehouseState.AVAILABLE,
                warehouseMgr.getWarehouse("warehouse_1").getState());

        Deencapsulation.setField(warehouse, "state", Warehouse.WarehouseState.SUSPENDED);

        warehouseMgr.replayAlterWarehouse(warehouse);
        Assert.assertEquals(Warehouse.WarehouseState.SUSPENDED, warehouseMgr.getWarehouse("warehouse_1").getState());

        warehouseMgr.replayDropWarehouse(new DropWarehouseLog("warehouse_1"));
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));
    }


    @Test
    public void testLoadWarehouse() throws IOException, DdlException {
        WarehouseManager warehouseMgr = GlobalStateMgr.getServingState().getWarehouseMgr();
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        warehouseMgr.saveWarehouses(out, 0);

        out.flush();
        out.close();

        Deencapsulation.setField(warehouseMgr, "nameToWh", new HashMap<>());
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        warehouseMgr.loadWarehouses(in, 0);
    }

    @Test
    public void testNewLoadWarehouse(@Mocked StarOSAgentEpack starOSAgent, @Mocked EditLog editLog)
            throws IOException, DdlException {
        WarehouseManagerEpack warehouseMgr = (WarehouseManagerEpack) GlobalStateMgr.getCurrentState().getWarehouseMgr();
        File file = new File(fileName);
        file.createNewFile();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getCurrentStarOSAgent() {
                return starOSAgent;
            }

            @Mock
            public long getNextId() {
                return 10000;
            }

            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };

        new Expectations() {
            {
                starOSAgent.deleteWorkerGroup(anyLong);
                result = null;
                minTimes = 0;

                starOSAgent.createWorkerGroup(anyString);
                result = -1L;
                minTimes = 0;
            }
        };

        warehouseMgr.createWarehouse(new CreateWarehouseStmt(false, "aaa", null, null));

        Assert.assertEquals(Warehouse.WarehouseState.AVAILABLE, warehouseMgr.getWarehouse("aaa").getState());
        warehouseMgr.suspendWarehouse(new SuspendWarehouseStmt("aaa"));
        Assert.assertEquals(Warehouse.WarehouseState.SUSPENDED, warehouseMgr.getWarehouse("aaa").getState());
        warehouseMgr.resumeWarehouse(new ResumeWarehouseStmt("aaa"));
        Assert.assertEquals(Warehouse.WarehouseState.AVAILABLE, warehouseMgr.getWarehouse("aaa").getState());

        warehouseMgr.dropWarehouse(new DropWarehouseStmt(false, "aaa"));
        Assert.assertFalse(warehouseMgr.warehouseExists("aaa"));

        Deencapsulation.setField(warehouseMgr, "nameToWh", new HashMap<>());
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        warehouseMgr.loadWarehouses(in, 0);
    }
}
