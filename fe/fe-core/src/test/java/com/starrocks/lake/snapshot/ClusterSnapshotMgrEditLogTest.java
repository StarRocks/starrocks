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
import com.starrocks.sql.ast.AdminAlterAutomatedSnapshotIntervalStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ClusterSnapshotMgrEditLogTest {
    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSetAutomatedSnapshotOnEditLogAndReplay() throws Exception {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        AdminSetAutomatedSnapshotOnStmt stmt = new AdminSetAutomatedSnapshotOnStmt("sv_on", null);
        stmt.setIntervalSeconds(120);

        mgr.setAutomatedSnapshotOn(stmt);
        Assertions.assertEquals("sv_on", mgr.getAutomatedSnapshotSvName());
        Assertions.assertEquals(120, mgr.getAutomatedSnapshotIntervalSeconds());

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_ON, log.getType());
        Assertions.assertEquals("sv_on", log.getStorageVolumeName());
        Assertions.assertEquals(120, log.getAutomatedSnapshotIntervalSeconds());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.replayLog(log);
        Assertions.assertEquals("sv_on", follower.getAutomatedSnapshotSvName());
        Assertions.assertEquals(120, follower.getAutomatedSnapshotIntervalSeconds());
    }

    @Test
    public void testSetAutomatedSnapshotOffEditLogAndReplay() throws Exception {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.setAutomatedSnapshotOn("sv_off", 60);

        AdminSetAutomatedSnapshotOffStmt stmt = new AdminSetAutomatedSnapshotOffStmt();
        mgr.setAutomatedSnapshotOff(stmt);
        Assertions.assertNull(mgr.getAutomatedSnapshotSvName());

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF, log.getType());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.setAutomatedSnapshotOn("sv_off", 60);
        follower.replayLog(log);
        Assertions.assertNull(follower.getAutomatedSnapshotSvName());
    }

    @Test
    public void testSetAutomatedSnapshotIntervalEditLogAndReplay() throws Exception {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        AdminAlterAutomatedSnapshotIntervalStmt stmt = new AdminAlterAutomatedSnapshotIntervalStmt(null);
        stmt.setIntervalSeconds(300);

        mgr.setAutomatedSnapshotInterval(stmt);
        Assertions.assertEquals(300, mgr.getAutomatedSnapshotIntervalSeconds());

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_INTERVAL, log.getType());
        Assertions.assertEquals(300, log.getAutomatedSnapshotIntervalSeconds());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.setAutomatedSnapshotInterval(60);
        follower.replayLog(log);
        Assertions.assertEquals(300, follower.getAutomatedSnapshotIntervalSeconds());
    }

    @Test
    public void testCreateAutomatedSnapshotJobEditLogAndReplay() throws Exception {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.setAutomatedSnapshotOn("sv_job", 60);

        ClusterSnapshotJob job = mgr.createAutomatedSnapshotJob();
        Assertions.assertNotNull(job);
        Assertions.assertEquals("sv_job", job.getStorageVolumeName());
        Assertions.assertEquals(ClusterSnapshotJobState.INITIALIZING, job.getState());
        Assertions.assertTrue(job.getSnapshotName().startsWith(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX));

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.UPDATE_SNAPSHOT_JOB, log.getType());
        Assertions.assertNotNull(log.getSnapshotJob());
        Assertions.assertEquals(job.getId(), log.getSnapshotJob().getId());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.replayLog(log);
        ClusterSnapshotJob replayed = follower.getClusterSnapshotJobByName(job.getSnapshotName());
        Assertions.assertNotNull(replayed);
        Assertions.assertEquals(job.getId(), replayed.getId());
        Assertions.assertEquals(ClusterSnapshotJobState.INITIALIZING, replayed.getState());
        Assertions.assertEquals("sv_job", replayed.getStorageVolumeName());
    }

    @Test
    public void testSetAutomatedSnapshotOnEditLogException() {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        AdminSetAutomatedSnapshotOnStmt stmt = new AdminSetAutomatedSnapshotOnStmt("sv_fail_on", null);
        stmt.setIntervalSeconds(90);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logClusterSnapshotLog(any(ClusterSnapshotLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> mgr.setAutomatedSnapshotOn(stmt));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertNull(mgr.getAutomatedSnapshotSvName());
            Assertions.assertEquals(0, mgr.getAutomatedSnapshotIntervalSeconds());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testSetAutomatedSnapshotOffEditLogException() {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.setAutomatedSnapshotOn("sv_fail_off", 30);
        AdminSetAutomatedSnapshotOffStmt stmt = new AdminSetAutomatedSnapshotOffStmt();

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logClusterSnapshotLog(any(ClusterSnapshotLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> mgr.setAutomatedSnapshotOff(stmt));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals("sv_fail_off", mgr.getAutomatedSnapshotSvName());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testSetAutomatedSnapshotIntervalEditLogException() {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.setAutomatedSnapshotInterval(45);
        AdminAlterAutomatedSnapshotIntervalStmt stmt = new AdminAlterAutomatedSnapshotIntervalStmt(null);
        stmt.setIntervalSeconds(200);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logClusterSnapshotLog(any(ClusterSnapshotLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> mgr.setAutomatedSnapshotInterval(stmt));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(45, mgr.getAutomatedSnapshotIntervalSeconds());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testCreateAutomatedSnapshotJobEditLogException() {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.setAutomatedSnapshotOn("sv_fail_job", 60);

        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logClusterSnapshotLog(any(ClusterSnapshotLog.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    mgr::createAutomatedSnapshotJob);
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertTrue(mgr.automatedSnapshotJobs.isEmpty());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }
}
