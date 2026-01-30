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

package com.starrocks.load.loadv2;

import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class InsertLoadJobEditLogTest {
    private static final long DB_ID = 10001L;
    private static final long TABLE_ID = 20001L;
    private static final long TXN_ID = 30001L;
    private static final String LOAD_ID = "load_id";
    private static final String USER = "test_user";
    private static final long TIMEOUT = 3600L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSetLoadFinishOrCancelAndReplay() throws Exception {
        InsertLoadJob job = createLoadingJob("finish_label");
        GlobalStateMgr.getCurrentState().getEditLog().logCreateLoadJob(job, wal -> {
        });

        String trackingUrl = "http://tracking/finish";
        job.setLoadFinishOrCancel("", trackingUrl);

        Assertions.assertEquals(JobState.FINISHED, job.getState());
        Assertions.assertEquals(100, job.getProgress());
        Assertions.assertEquals(trackingUrl, job.loadingStatus.getTrackingUrl());

        LoadMgr followerLoadMgr = new LoadMgr(new LoadJobScheduler());
        LoadJob replayCreateJob = (LoadJob) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_LOAD_JOB_V2);
        followerLoadMgr.replayCreateLoadJob(replayCreateJob);

        LoadJobFinalOperation operation = (LoadJobFinalOperation) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_END_LOAD_JOB_V2);
        Assertions.assertEquals(job.getId(), operation.getId());
        Assertions.assertEquals(JobState.FINISHED, operation.getJobState());
        Assertions.assertEquals(100, operation.getProgress());
        Assertions.assertEquals(trackingUrl, operation.getLoadingStatus().getTrackingUrl());
        Assertions.assertNull(operation.getFailMsg());

        followerLoadMgr.replayEndLoadJob(operation);
        LoadJob followerJob = followerLoadMgr.getLoadJob(job.getId());
        Assertions.assertNotNull(followerJob);
        Assertions.assertEquals(JobState.FINISHED, followerJob.getState());
        Assertions.assertEquals(100, followerJob.getProgress());
        Assertions.assertEquals(trackingUrl, followerJob.loadingStatus.getTrackingUrl());
    }

    @Test
    public void testSetLoadFinishOrCancelEditLogException() throws Exception {
        InsertLoadJob job = createLoadingJob("exception_label");
        EditLog originalEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(originalEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logEndLoadJob(any(LoadJobFinalOperation.class), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> job.setLoadFinishOrCancel("fail", "http://tracking/fail"));
            Assertions.assertEquals("EditLog write failed", exception.getMessage());
            Assertions.assertEquals(JobState.LOADING, job.getState());
            Assertions.assertEquals(0, job.getProgress());
            Assertions.assertNotNull(job.failMsg);
        } finally {
            GlobalStateMgr.getCurrentState().setEditLog(originalEditLog);
        }
    }

    private static InsertLoadJob createLoadingJob(String label) {
        Coordinator coordinator = mock(Coordinator.class);
        when(coordinator.getLoadJobType()).thenReturn(TLoadJobType.INSERT_QUERY);
        InsertLoadTxnCallback insertLoadTxnCallback = mock(InsertLoadTxnCallback.class);
        long createTimestamp = System.currentTimeMillis();
        return new InsertLoadJob(label, DB_ID, TABLE_ID, TXN_ID, LOAD_ID, USER, createTimestamp,
                Math.max(TIMEOUT, Config.insert_load_default_timeout_second),
                0L, coordinator, insertLoadTxnCallback);
    }
}
