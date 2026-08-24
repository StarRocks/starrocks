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

import com.google.gson.JsonObject;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminAlterAutomatedSnapshotIntervalStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

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
        AdminSetAutomatedSnapshotOnStmt stmt = new AdminSetAutomatedSnapshotOnStmt("sv_on", null, new HashMap<>());
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
        mgr.setAutomatedSnapshotOn("sv_off", 60, new HashMap<>());

        AdminSetAutomatedSnapshotOffStmt stmt = new AdminSetAutomatedSnapshotOffStmt();
        mgr.setAutomatedSnapshotOff(stmt);
        Assertions.assertNull(mgr.getAutomatedSnapshotSvName());

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF, log.getType());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.setAutomatedSnapshotOn("sv_off", 60, new HashMap<>());
        follower.replayLog(log);
        Assertions.assertNull(follower.getAutomatedSnapshotSvName());
    }

    @Test
    public void testResetSnapshotStateAfterExternalRestoreEditLogAndReplay() throws Exception {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.setAutomatedSnapshotOn("sv_external", 60, new HashMap<>());
        mgr.addSnapshotJob(new ExternalClusterSnapshotJob(
                1L, "automated_cluster_snapshot_1", "sv_external", 1L));

        mgr.resetSnapshotStateAfterExternalRestore();
        Assertions.assertNull(mgr.getAutomatedSnapshotSvName());
        Assertions.assertTrue(mgr.getAutomatedSnapshotJobs().isEmpty());

        ClusterSnapshotLog log = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);
        // Written as a record type every released FE knows, with the extra reset carried by a flag.
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF, log.getType());
        Assertions.assertTrue(log.isResetInheritedSnapshotState());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.setAutomatedSnapshotOn("sv_external", 60, new HashMap<>());
        follower.addSnapshotJob(new ExternalClusterSnapshotJob(
                1L, "automated_cluster_snapshot_1", "sv_external", 1L));
        follower.replayLog(log);
        Assertions.assertNull(follower.getAutomatedSnapshotSvName());
        Assertions.assertTrue(follower.getAutomatedSnapshotJobs().isEmpty());
    }

    @Test
    public void testResetSnapshotStateDowngradeReplayKeepsLegacyBehavior() throws Exception {
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        mgr.resetSnapshotStateAfterExternalRestore();
        ClusterSnapshotLog resetLog = (ClusterSnapshotLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CLUSTER_SNAPSHOT_LOG);

        // Model an older FE schema dropping the unknown reset flag from the actual journal payload.
        JsonObject legacyJson = GsonUtils.GSON.toJsonTree(resetLog).getAsJsonObject();
        Assertions.assertTrue(legacyJson.remove("resetInheritedSnapshotState").getAsBoolean());
        ClusterSnapshotLog legacyLog = GsonUtils.GSON.fromJson(legacyJson, ClusterSnapshotLog.class);
        Assertions.assertEquals(ClusterSnapshotLog.ClusterSnapshotLogType.AUTOMATED_SNAPSHOT_OFF,
                legacyLog.getType());
        Assertions.assertFalse(legacyLog.isResetInheritedSnapshotState());

        ClusterSnapshotMgrEPack legacyFollower = new ClusterSnapshotMgrEPack();
        legacyFollower.setAutomatedSnapshotOn("sv_external", 60, new HashMap<>());
        legacyFollower.addSnapshotJob(new ExternalClusterSnapshotJob(
                1L, "automated_cluster_snapshot_1", "sv_external", 1L));
        legacyFollower.addManualClusterSnapshotRequest(
                new ManualClusterSnapshotRequest("manual_request", "sv_external"));
        legacyFollower.addManualClusterSnapshotJob(
                new ManualClusterSnapshotJob(2L, "manual_job", "sv_external", 2L));

        Assertions.assertDoesNotThrow(() -> legacyFollower.replayLog(legacyLog));
        Assertions.assertNull(legacyFollower.getAutomatedSnapshotSvName());
        Assertions.assertEquals(1, legacyFollower.getAutomatedSnapshotJobs().size());
        Assertions.assertEquals(1, legacyFollower.getManualClusterSnapshotRequestQueue().size());
        Assertions.assertEquals(1, legacyFollower.getManualClusterSnapshotJobs().size());
    }

    @Test
    public void testReplayLogWithUnknownLogTypeIsSkipped() {
        // A record written by a newer FE: gson maps the unknown enum value to null, and replay must
        // skip the record instead of throwing and aborting the journal.
        ClusterSnapshotLog log = GsonUtils.GSON.fromJson("{\"type\":\"SOME_FUTURE_LOG_TYPE\"}",
                ClusterSnapshotLog.class);
        Assertions.assertNull(log.getType());

        ClusterSnapshotMgr follower = new ClusterSnapshotMgr();
        follower.setAutomatedSnapshotOn("sv_external", 60, new HashMap<>());
        follower.replayLog(log);
        Assertions.assertEquals("sv_external", follower.getAutomatedSnapshotSvName());
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
        mgr.setAutomatedSnapshotOn("sv_job", 60, new HashMap<>());

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
        AdminSetAutomatedSnapshotOnStmt stmt = new AdminSetAutomatedSnapshotOnStmt("sv_fail_on", null, new HashMap<>());
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
        mgr.setAutomatedSnapshotOn("sv_fail_off", 30, new HashMap<>());
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
        mgr.setAutomatedSnapshotOn("sv_fail_job", 60, new HashMap<>());

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
