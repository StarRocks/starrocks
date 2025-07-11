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
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.HistoricalNodeSet;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HistoricalNodeProcNode implements ProcNodeInterface {
    private static final List<String> TITLES = Collections.unmodifiableList(Arrays.asList(
            "Warehouse", "WorkerGroupId", "BackendIDs", "ComputeNodeIDs", "UpdateTime"));

    private GlobalStateMgr globalStateMgr;

    public HistoricalNodeProcNode(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLES);
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        HistoricalNodeMgr historicalNodeMgr = globalStateMgr.getHistoricalNodeMgr();
        ConcurrentHashMap<Pair<Long, Long>, HistoricalNodeSet> resourceToNodeSet = historicalNodeMgr.getAllHistoricalNodeSet();
        for (Pair<Long, Long> resourceId : resourceToNodeSet.keySet()) {
            HistoricalNodeSet nodeSet = resourceToNodeSet.get(resourceId);
            Warehouse warehouse = warehouseManager.getWarehouse(resourceId.first);
            if (nodeSet == null || warehouse == null) {
                continue;
            }
            List<String> row = new ArrayList<>();
            row.add(warehouse.getName());
            row.add(resourceId.second.toString());
            row.add(nodeSet.getHistoricalBackendIds().toString());
            row.add(nodeSet.getHistoricalComputeNodeIds().toString());
            row.add(TimeUtils.longToTimeString(nodeSet.getLastUpdateTime()));
            result.addRow(row);
        }
        return result;
    }
}
