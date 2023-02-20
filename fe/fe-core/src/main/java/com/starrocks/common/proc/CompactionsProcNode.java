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
import com.starrocks.lake.compaction.CompactionContext;
import com.starrocks.lake.compaction.CompactionManager;
import com.starrocks.lake.compaction.PartitionIdentifier;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class CompactionsProcNode implements ProcNodeInterface {
    private List<String> titles = new ArrayList<>();

    public CompactionsProcNode() {
        titles.add("Partition");
        titles.add("TxnID");
        titles.add("StartTime");
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(titles);
        CompactionManager compactionManager = GlobalStateMgr.getCurrentState().getCompactionManager();
        if (compactionManager == null) {
            return result;
        }
        ConcurrentHashMap<PartitionIdentifier, CompactionContext> runningCompactions = compactionManager.getRunningCompactions();
        for (CompactionContext context : runningCompactions.values()) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(context.getFullPartitionName()));
            row.add(String.valueOf(context.getTxnId()));
            row.add(TimeUtils.longToTimeString(context.getStartTs()));

            result.addRow(row);
        }
        return result;
    }
}
