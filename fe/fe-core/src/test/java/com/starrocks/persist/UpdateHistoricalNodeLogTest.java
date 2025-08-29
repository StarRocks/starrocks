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

import com.starrocks.common.io.Text;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.SystemInfoService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class UpdateHistoricalNodeLogTest {

    private String fileName = "./UpdateHistoricalNodeLogTest";

    @AfterEach
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        long workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
        List<Long> backendIds = Arrays.asList(101L, 102L, 103L);
        List<Long> computeNodeIds = Arrays.asList(201L, 202L, 203L);
        long updateTime = System.currentTimeMillis();

        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));
        UpdateHistoricalNodeLog writeLog =
                new UpdateHistoricalNodeLog(warehouseId, workerGroupId, updateTime, backendIds, computeNodeIds);
        Text.writeString(out, GsonUtils.GSON.toJson(writeLog, UpdateHistoricalNodeLog.class));
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        UpdateHistoricalNodeLog readLog = GsonUtils.GSON.fromJson(Text.readString(in), UpdateHistoricalNodeLog.class);
        Assertions.assertEquals(readLog.getWarehouseId(), warehouseId);
        Assertions.assertEquals(readLog.getWorkerGroupId(), workerGroupId);
        Assertions.assertEquals(readLog.getUpdateTime(), updateTime);
        Assertions.assertEquals(readLog.getBackendIds(), backendIds);
        Assertions.assertEquals(readLog.getComputeNodeIds(), computeNodeIds);
        in.close();

        // 3. replay the log
        SystemInfoService systemInfoService = new SystemInfoService();
        HistoricalNodeMgr historicalNodeMgr = GlobalStateMgr.getCurrentState().getHistoricalNodeMgr();
        systemInfoService.replayUpdateHistoricalNode(readLog);
        Assertions.assertEquals(historicalNodeMgr.getHistoricalBackendIds(warehouseId, workerGroupId).size(), backendIds.size());
        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouseId, workerGroupId).size(),
                computeNodeIds.size());
    }
}
