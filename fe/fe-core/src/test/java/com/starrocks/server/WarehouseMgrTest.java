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
import com.starrocks.lake.StarOSAgent;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.DropWarehouseStmt;
import com.starrocks.sql.ast.ResumeWarehouseStmt;
import com.starrocks.sql.ast.SuspendWarehouseStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.warehouse.Warehouse;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WarehouseMgrTest {
    private static StarRocksAssert starRocksAssert;
    private String fileName = "./testWarehouseMgr";

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        String createWarehouse = "CREATE WAREHOUSE aaa";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withWarehouse(createWarehouse);
    }

    @After
    public void tearDownCreate() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testReplay(@Mocked StarOSAgent starOSAgent) throws Exception {

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

        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Map<String, String> properties = new HashMap<>();
        properties.put("min_cluster", "2");
        properties.put("size", "large");

        Warehouse warehouse = new Warehouse(10000, "warehouse_1", properties);
        warehouseMgr.replayCreateWarehouse(warehouse);
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));
        Assert.assertEquals(Warehouse.WarehouseState.INITIALIZING,
                warehouseMgr.getWarehouse("warehouse_1").getState());

        warehouseMgr.replayDropWarehouse("warehouse_1");
        Assert.assertFalse(warehouseMgr.warehouseExists("warehouse_1"));

        Map<String, Warehouse> tempMp = new HashMap<>();
        tempMp.put("warehouse_1", new Warehouse());
        Deencapsulation.setField(warehouseMgr, "fullNameToWh", tempMp);

        warehouseMgr.replaySuspendWarehouse("warehouse_1");
        Assert.assertEquals(Warehouse.WarehouseState.SUSPENDED, warehouseMgr.getWarehouse("warehouse_1").getState());

        warehouseMgr.replayResumeWarehouse("warehouse_1", null);
        Assert.assertEquals(Warehouse.WarehouseState.RUNNING, warehouseMgr.getWarehouse("warehouse_1").getState());
    }

    @Test
    public void testLoadWarehouse(@Mocked StarOSAgent starOSAgent) throws IOException, DdlException {
        WarehouseManager warehouseMgr = GlobalStateMgr.getServingState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists("aaa"));

        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        warehouseMgr.saveWarehouses(out, 0);

        out.flush();
        out.close();

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


        Assert.assertEquals(Warehouse.WarehouseState.INITIALIZING, warehouseMgr.getWarehouse("aaa").getState());
        warehouseMgr.suspendWarehouse(new SuspendWarehouseStmt("aaa"));
        Assert.assertEquals(Warehouse.WarehouseState.SUSPENDED, warehouseMgr.getWarehouse("aaa").getState());
        warehouseMgr.resumeWarehouse(new ResumeWarehouseStmt("aaa"));
        Assert.assertEquals(Warehouse.WarehouseState.RUNNING, warehouseMgr.getWarehouse("aaa").getState());
        Assert.assertEquals(warehouseMgr.getWarehouse("aaa").getMinCluster(),
                warehouseMgr.getWarehouse("aaa").getClusters().size());

        warehouseMgr.dropWarehouse(new DropWarehouseStmt(false, "aaa"));
        Assert.assertFalse(warehouseMgr.warehouseExists("aaa"));

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        warehouseMgr.loadWarehouses(in, 0);
        Assert.assertTrue(warehouseMgr.warehouseExists("aaa"));
    }
}
