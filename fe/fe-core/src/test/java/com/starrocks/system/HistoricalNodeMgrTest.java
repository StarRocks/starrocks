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

import com.google.gson.stream.JsonReader;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class HistoricalNodeMgrTest {
    @BeforeEach
    public void setUp() throws IOException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.initDefaultWarehouse();
    }

    @Test
    public void testUpdateHistoricalBackendIds() throws Exception {
        HistoricalNodeMgr historicalNodeMgr = new HistoricalNodeMgr();
        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

        List<Long> backendIds = Arrays.asList(101L, 102L, 103L);
        long updateTime = System.currentTimeMillis();
        historicalNodeMgr.updateHistoricalBackendIds(backendIds, updateTime, warehouse);

        Assertions.assertEquals(historicalNodeMgr.getHistoricalBackendIds(warehouse).size(), backendIds.size());
        Assertions.assertEquals(historicalNodeMgr.getLastUpdateTime(warehouse), updateTime);
        Assertions.assertEquals(historicalNodeMgr.getAllHistoricalNodeSet().size(), 1);
    }

    @Test
    public void testUpdateHistoricalComputeNodeIds() throws Exception {
        HistoricalNodeMgr historicalNodeMgr = new HistoricalNodeMgr();
        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

        List<Long> computeNodeIds = Arrays.asList(201L, 202L, 203L);
        long updateTime = System.currentTimeMillis();
        historicalNodeMgr.updateHistoricalComputeNodeIds(computeNodeIds, updateTime, warehouse);

        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouse).size(), computeNodeIds.size());
        Assertions.assertEquals(historicalNodeMgr.getLastUpdateTime(warehouse), updateTime);
        Assertions.assertEquals(historicalNodeMgr.getAllHistoricalNodeSet().size(), 1);
    }

    @Test
    public void testNonExistWarehouseNodeSet() throws Exception {
        HistoricalNodeMgr historicalNodeMgr = new HistoricalNodeMgr();
        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

        Assertions.assertEquals(historicalNodeMgr.getHistoricalBackendIds(warehouse).size(), 0);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouse).size(), 0);
        Assertions.assertEquals(historicalNodeMgr.getLastUpdateTime(warehouse), 0);
    }

    @Test
    public void testSaveAndLoadImage() throws Exception {
        HistoricalNodeMgr historicalNodeMgr = new HistoricalNodeMgr();
        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

        List<Long> computeNodeIds = Arrays.asList(201L, 202L, 203L);
        long updateTime = System.currentTimeMillis();
        historicalNodeMgr.updateHistoricalComputeNodeIds(computeNodeIds, updateTime, warehouse);
        Assertions.assertEquals(historicalNodeMgr.getAllHistoricalNodeSet().size(), 1);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouse).size(), computeNodeIds.size());
        Assertions.assertEquals(historicalNodeMgr.getLastUpdateTime(warehouse), updateTime);

        File tempFile = File.createTempFile("HistoricalNodeMgrTest", ".image");

        // save image
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        ImageWriter imageWriter = new ImageWriter("", 0);
        imageWriter.setOutputStream(dos);
        historicalNodeMgr.save(imageWriter);
        dos.close();

        // reset historical node manager
        historicalNodeMgr.clear();
        Assertions.assertEquals(historicalNodeMgr.getAllHistoricalNodeSet().size(), 0);

        // load content from image
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        SRMetaBlockReader srMetaBlockReader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(dis)));
        historicalNodeMgr.load(srMetaBlockReader);
        dis.close();
        Assertions.assertEquals(historicalNodeMgr.getAllHistoricalNodeSet().size(), 1);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouse).size(), computeNodeIds.size());
        Assertions.assertEquals(historicalNodeMgr.getLastUpdateTime(warehouse), updateTime);

        tempFile.delete();
    }
}
