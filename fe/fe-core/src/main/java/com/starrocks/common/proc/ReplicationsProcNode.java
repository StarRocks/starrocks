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

import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ReplicationsProcNode implements ProcNodeInterface {
    private static final List<String> TITLES = Collections.unmodifiableList(Arrays.asList(
            "JobID", "DatabaseID", "TableID", "TxnID", "CreatedTime", "FinishedTime", "State", "Progress", "Error"));

    public ReplicationsProcNode() {
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLES);
        ReplicationMgr replicationMgr = GlobalStateMgr.getCurrentState().getReplicationMgr();
        List<ReplicationJob> runningJobs = new ArrayList<>(replicationMgr.getRunningJobs());
        List<ReplicationJob> abortedJobs = new ArrayList<>(replicationMgr.getAbortedJobs());
        List<ReplicationJob> committedJobs = new ArrayList<>(replicationMgr.getCommittedJobs());
        runningJobs.sort((job1, job2) -> Long.compare(job1.getCreatedTimeMs(), job2.getCreatedTimeMs()));
        abortedJobs.sort((job1, job2) -> Long.compare(job1.getCreatedTimeMs(), job2.getCreatedTimeMs()));
        committedJobs.sort((job1, job2) -> Long.compare(job1.getCreatedTimeMs(), job2.getCreatedTimeMs()));
        List<ReplicationJob> replicationJobs = new ArrayList<>();
        replicationJobs.addAll(runningJobs);
        replicationJobs.addAll(abortedJobs);
        replicationJobs.addAll(committedJobs);
        for (ReplicationJob job : replicationJobs) {
            List<String> row = new ArrayList<>();
            row.add(job.getJobId());
            row.add(String.valueOf(job.getDatabaseId()));
            row.add(String.valueOf(job.getTableId()));
            row.add(String.valueOf(job.getTransactionId()));
            row.add(TimeUtils.longToTimeString(job.getCreatedTimeMs()));
            row.add(TimeUtils.longToTimeString(job.getFinishedTimeMs()));
            row.add(job.getState().toString());
            row.add(job.getState().isRunning() ? (job.getRunningTaskNum() + "/" + job.getTotalTaskNum()) : "");
            row.add(Strings.nullToEmpty(job.getErrorMessage()));
            result.addRow(row);
        }
        return result;
    }
}
