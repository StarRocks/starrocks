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
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;
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
    public void testReplay() throws Exception {

        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = new LocalWarehouse(10000, "warehouse_1");
        warehouseMgr.replayCreateWarehouse(warehouse);
        Assert.assertTrue(warehouseMgr.warehouseExists("warehouse_1"));
        Assert.assertEquals(Warehouse.WarehouseState.INITIALIZING,
                warehouseMgr.getWarehouse("warehouse_1").getState());

        Map<String, Warehouse> tempMp = new HashMap<>();
        tempMp.put("warehouse_1", new LocalWarehouse(0, "warehouse_1"));
        Deencapsulation.setField(warehouseMgr, "fullNameToWh", tempMp);
    }

    @Test
    public void testLoadWarehouse() throws IOException, DdlException {
        WarehouseManager warehouseMgr = GlobalStateMgr.getServingState().getWarehouseMgr();
        Assert.assertTrue(warehouseMgr.warehouseExists("aaa"));

        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        warehouseMgr.saveWarehouses(out, 0);

        out.flush();
        out.close();

        Assert.assertEquals(Warehouse.WarehouseState.INITIALIZING, warehouseMgr.getWarehouse("aaa").getState());

        Deencapsulation.setField(warehouseMgr, "fullNameToWh", new HashMap<>());

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        warehouseMgr.loadWarehouses(in, 0);

        Assert.assertTrue(warehouseMgr.warehouseExists("aaa"));
    }
}
