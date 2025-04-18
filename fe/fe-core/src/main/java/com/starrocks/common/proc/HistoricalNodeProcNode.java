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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.HistoricalNodeSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HistoricalNodeProcNode implements ProcNodeInterface {
    private static final List<String> TITLES = Collections.unmodifiableList(Arrays.asList(
            "Warehouse", "BackendIDs", "ComputeNodeIDs", "UpdateTime"));

    private GlobalStateMgr globalStateMgr;

    public HistoricalNodeProcNode(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLES);
        HistoricalNodeMgr historicalNodeMgr = globalStateMgr.getHistoricalNodeMgr();
        ConcurrentHashMap<String, HistoricalNodeSet> whToNodeSet = historicalNodeMgr.getAllHistoricalNodeSet();
        for (String warehouse : whToNodeSet.keySet()) {
            HistoricalNodeSet nodeSet = whToNodeSet.get(warehouse);
            if (nodeSet == null) {
                continue;
            }
            List<String> row = new ArrayList<>();
            row.add(warehouse);
            row.add(nodeSet.getHistoricalBackendIds().toString());
            row.add(nodeSet.getHistoricalComputeNodeIds().toString());
            row.add(TimeUtils.longToTimeString(nodeSet.getLastUpdateTime()));
            result.addRow(row);
        }
        return result;
    }
}
