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

package com.starrocks.load;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ExportJobEditLogTest {
    private ExportJob masterExportJob;
    private static final long TEST_JOB_ID = 100L;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create ExportJob instance
        UUID queryId = UUIDUtil.genUUID();
        masterExportJob = new ExportJob(TEST_JOB_ID, queryId);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Update State Tests ====================

    @Test
    public void testUpdateStateNormalCase() throws Exception {
        // 1. Verify initial state
        Assertions.assertEquals(ExportJob.JobState.PENDING, masterExportJob.getState());

        // 2. Execute updateState operation
        long stateChangeTime = System.currentTimeMillis();
        boolean result = masterExportJob.updateState(ExportJob.JobState.EXPORTING, stateChangeTime);

        // 3. Verify master state
        Assertions.assertTrue(result);
        Assertions.assertEquals(ExportJob.JobState.EXPORTING, masterExportJob.getState());

        // 4. Test follower replay functionality
        ExportMgr followerExportMgr = new ExportMgr();
        ExportJob followerJob = new ExportJob(TEST_JOB_ID, masterExportJob.getQueryId());
        followerExportMgr.unprotectAddJob(followerJob);
        Assertions.assertEquals(ExportJob.JobState.PENDING, followerJob.getState());

        ExportJob.ExportUpdateInfo replayUpdateInfo = (ExportJob.ExportUpdateInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_EXPORT_UPDATE_INFO_V2);

        followerExportMgr.replayUpdateJobInfo(replayUpdateInfo);

        // 5. Verify follower state is consistent with master
        Assertions.assertEquals(ExportJob.JobState.EXPORTING, followerJob.getState());
    }

    @Test
    public void testUpdateStateEditLogException() throws Exception {
        // 1. Create a separate ExportJob for exception testing
        UUID queryId = UUIDUtil.genUUID();
        ExportJob exceptionExportJob = new ExportJob(TEST_JOB_ID, queryId);
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 2. Mock EditLog.logExportUpdateState to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logExportUpdateState(any(ExportJob.ExportUpdateInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertEquals(ExportJob.JobState.PENDING, exceptionExportJob.getState());

        // 3. Execute updateState operation and expect exception
        long stateChangeTime = System.currentTimeMillis();
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionExportJob.updateState(ExportJob.JobState.EXPORTING, stateChangeTime);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(ExportJob.JobState.PENDING, exceptionExportJob.getState());
    }
}

