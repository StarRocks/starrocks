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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.HistoricalNodeMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HistoricalNodeProcNodeTest {
    @BeforeEach
    public void setUp() throws IOException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.initDefaultWarehouse();

        HistoricalNodeMgr historicalNodeMgr = GlobalStateMgr.getCurrentState().getHistoricalNodeMgr();

        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        List<Long> computeNodeIds = Arrays.asList(201L, 202L);
        long updateTime = System.currentTimeMillis();
        historicalNodeMgr.updateHistoricalComputeNodeIds(computeNodeIds, updateTime, warehouse);

        Assertions.assertEquals(historicalNodeMgr.getHistoricalComputeNodeIds(warehouse).size(), computeNodeIds.size());
        Assertions.assertEquals(historicalNodeMgr.getLastUpdateTime(warehouse), updateTime);
        Assertions.assertEquals(historicalNodeMgr.getAllHistoricalNodeSet().size(), 1);
    }

    @Test
    public void testFetchResult() throws AnalysisException {
        HistoricalNodeProcNode node = new HistoricalNodeProcNode(GlobalStateMgr.getCurrentState());
        BaseProcResult result = (BaseProcResult) node.fetchResult();
        Assertions.assertNotNull(result);

        List<List<String>> rows = result.getRows();
        List<String> list1 = rows.get(0);
        Assertions.assertEquals(list1.size(), 4);
        // Warehouse
        Assertions.assertEquals(list1.get(0), WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        // BackendIds
        Assertions.assertEquals(list1.get(1), "[]");
        // ComputeNodeIds
        Assertions.assertEquals(list1.get(2), "[201, 202]");
        // UpdateTime
        Assertions.assertNotEquals(list1.get(3), "0");
    }
}
