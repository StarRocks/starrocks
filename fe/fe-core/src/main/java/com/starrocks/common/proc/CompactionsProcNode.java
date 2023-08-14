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
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.lake.compaction.CompactionRecord;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CompactionsProcNode implements ProcNodeInterface {
    private static final List<String> TITLES = Collections.unmodifiableList(Arrays.asList(
            "Partition", "TxnID", "StartTime", "CommitTime", "FinishTime", "Error"));

    public CompactionsProcNode() {
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLES);
        CompactionMgr compactionManager = GlobalStateMgr.getCurrentState().getCompactionMgr();
        List<CompactionRecord> history = compactionManager.getHistory();
        for (CompactionRecord record : history) {
            List<String> row = new ArrayList<>();
            row.add(record.getPartitionName());
            row.add(String.valueOf(record.getTxnId()));
            row.add(TimeUtils.longToTimeString(record.getStartTs()));
            row.add(record.getCommitTs().map(TimeUtils::longToTimeString).orElse(null));
            row.add(record.getFinishTs().map(TimeUtils::longToTimeString).orElse(null));
            row.add(record.getErrorMessage().orElse(null));
            result.addRow(row);
        }
        return result;
    }
}
