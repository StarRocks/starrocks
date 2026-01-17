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

package com.starrocks.backup;

import com.starrocks.backup.BackupJob.BackupJobState;
import com.starrocks.backup.RestoreJob.RestoreJobState;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class BackupRestoreEditLogTest {
    private static final long DB_ID = 10001L;
    private static final String DB_NAME = "test_db";

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testBackupJobPersistStateChangeAndReplayLog() throws Exception {
        BackupJob job = createBackupJob("backup_label");
        job.persistStateChange(BackupJobState.UPLOAD_INFO);
        Assertions.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());

        BackupJob logJob = (BackupJob) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_BACKUP_JOB_V2);
        Assertions.assertEquals(job.getJobId(), logJob.getJobId());
        Assertions.assertEquals(BackupJobState.UPLOAD_INFO, logJob.getState());
        Assertions.assertEquals(job.getLabel(), logJob.getLabel());
    }

    @Test
    public void testRestoreJobPersistStateChangeAndReplayLog() throws Exception {
        RestoreJob job = createRestoreJob("restore_label");
        job.persistStateChange(RestoreJobState.DOWNLOAD);
        Assertions.assertEquals(RestoreJobState.DOWNLOAD, job.getState());

        RestoreJob logJob = (RestoreJob) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_RESTORE_JOB_V2);
        Assertions.assertEquals(job.getJobId(), logJob.getJobId());
        Assertions.assertEquals(RestoreJobState.DOWNLOAD, logJob.getState());
        Assertions.assertEquals(job.getLabel(), logJob.getLabel());
    }

    @Test
    public void testBackupJobPersistStateChangeEditLogException() {
        BackupJob job = createBackupJob("backup_exception");
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logBackupJob(any(BackupJob.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> job.persistStateChange(BackupJobState.UPLOAD_INFO));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(BackupJobState.PENDING, job.getState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    @Test
    public void testRestoreJobPersistStateChangeEditLogException() {
        RestoreJob job = createRestoreJob("restore_exception");
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logRestoreJob(any(RestoreJob.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> job.persistStateChange(RestoreJobState.DOWNLOAD));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(RestoreJobState.PENDING, job.getState());
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private static BackupJob createBackupJob(String label) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        return new BackupJob(label, DB_ID, DB_NAME, new ArrayList<>(), 1000L, globalStateMgr, 1L);
    }

    private static RestoreJob createRestoreJob(String label) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        BackupJobInfo jobInfo = new BackupJobInfo();
        BackupMeta backupMeta = new BackupMeta(new ArrayList<>());
        return new RestoreJob(label, "backup_ts", DB_ID, DB_NAME, jobInfo, false, 3, 1000L,
                globalStateMgr, 1L, backupMeta, new MvRestoreContext());
    }
}
