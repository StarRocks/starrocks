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

package com.starrocks.persist;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.SystemInfoService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class UpdateHistoricalNodeLogTest {

    private String fileName = "./UpdateHistoricalNodeLogTest";

    @After
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        List<Long> backendIds = Arrays.asList(101L, 102L, 103L);
        List<Long> computeNodeIds = Arrays.asList(201L, 202L, 203L);
        long updateTime = System.currentTimeMillis();

        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));
        UpdateHistoricalNodeLog writeLog =
                new UpdateHistoricalNodeLog(warehouse, updateTime, backendIds, computeNodeIds);
        writeLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        UpdateHistoricalNodeLog readLog = UpdateHistoricalNodeLog.read(in);
        Assert.assertEquals(readLog.getWarehouse(), warehouse);
        Assert.assertEquals(readLog.getUpdateTime(), updateTime);
        Assert.assertEquals(readLog.getBackendIds(), backendIds);
        Assert.assertEquals(readLog.getComputeNodeIds(), computeNodeIds);
        in.close();

        // 3. replay the log
        SystemInfoService systemInfoService = new SystemInfoService();
        HistoricalNodeMgr historicalNodeMgr = GlobalStateMgr.getCurrentState().getHistoricalNodeMgr();
        systemInfoService.replayUpdateHistoricalNode(readLog);
        Assert.assertEquals(historicalNodeMgr.getHistoricalBackendIds(warehouse).size(), backendIds.size());
        Assert.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouse).size(), computeNodeIds.size());
    }
}