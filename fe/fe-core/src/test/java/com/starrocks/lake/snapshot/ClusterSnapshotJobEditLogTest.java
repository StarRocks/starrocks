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

package com.starrocks.lake.snapshot;

import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TClusterSnapshotJobsItem;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ClusterSnapshotJobEditLogTest {
    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testPersistStateChangeAndReplay() throws Exception {
        long jobId = 101L;
        ClusterSnapshotJob job = new ClusterSnapshotJob(jobId, "snapshot_editlog", "sv", System.currentTimeMillis());

        ClusterSnapshotJob.persistStateChange(job, ClusterSnapshotJobState.SNAPSHOTING);
        Assertions.assertEquals(ClusterSnapshotJobState.SNAPSHOTING, job.getState());

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.UPDATE_SNAPSHOT_JOB, log.getType());
        Assertions.assertNotNull(log.getSnapshotJob());
        Assertions.assertEquals(jobId, log.getSnapshotJob().getId());
        Assertions.assertEquals(ClusterSnapshotJobState.SNAPSHOTING, log.getSnapshotJob().getState());

        ClusterSnapshotMgr mgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
        mgr.replayLog(log);

        TClusterSnapshotJobsResponse response = mgr.getAllSnapshotJobsInfo();
        Optional<TClusterSnapshotJobsItem> replayed = response.getItems().stream()
                .filter(item -> item.getJob_id() == jobId)
                .findFirst();
        Assertions.assertTrue(replayed.isPresent());
        Assertions.assertEquals(ClusterSnapshotJobState.SNAPSHOTING.name(), replayed.get().getState());
    }

    @Test
    public void testPersistStateChangeEditLogException() {
        ClusterSnapshotJob job = new ClusterSnapshotJob(102L, "snapshot_editlog_exception", "sv",
                System.currentTimeMillis());

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logClusterSnapshotLog(any(ClusterSnapshotLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> ClusterSnapshotJob.persistStateChange(job, ClusterSnapshotJobState.SNAPSHOTING));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(ClusterSnapshotJobState.INITIALIZING, job.getState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }
}
