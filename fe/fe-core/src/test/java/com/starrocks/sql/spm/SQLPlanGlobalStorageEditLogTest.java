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

package com.starrocks.sql.spm;

import com.starrocks.common.FeConstants;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SQLPlanGlobalStorageEditLogTest {
    private SQLPlanGlobalStorage storage;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Mock SimpleExecutor to avoid DML execution failures in test environment
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                // Do nothing in test - just skip DML execution
            }
        };

        storage = new SQLPlanGlobalStorage();

        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testStoreBaselinePlanNormalCase() throws Exception {
        // 1. Prepare test data
        List<BaselinePlan> plans = new ArrayList<>();
        BaselinePlan plan1 = new BaselinePlan("SELECT * FROM t1 WHERE id = ?", "SELECT * FROM t1 WHERE id = 1", 
                12345L, "SELECT * FROM t1 WHERE id = 1", 100.0);
        plan1.setId(GlobalStateMgr.getCurrentState().getNextId());
        plans.add(plan1);

        // 2. Execute storeBaselinePlan operation (master side)
        storage.storeBaselinePlan(plans);

        // 3. Verify master state - plan should be stored
        Set<Long> foundIds = storage.getAllBaselineIds();
        Assertions.assertTrue(foundIds.contains(plan1.getId()));

        // 4. Test follower replay functionality
        SQLPlanGlobalStorage followerStorage = new SQLPlanGlobalStorage();
        
        // Verify follower initial state
        Set<Long> followerIds = followerStorage.getAllBaselineIds();
        Assertions.assertFalse(followerIds.contains(plan1.getId()));

        // Replay the operation
        BaselinePlan.Info replayInfo = (BaselinePlan.Info) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_SPM_BASELINE_LOG);
        followerStorage.replayBaselinePlan(replayInfo, true);

        // 5. Verify follower state is consistent with master
        followerIds = followerStorage.getAllBaselineIds();
        Assertions.assertTrue(followerIds.contains(plan1.getId()));
    }

    @Test
    public void testStoreBaselinePlanEditLogException() throws Exception {
        // 1. Prepare test data
        List<BaselinePlan> plans = new ArrayList<>();
        BaselinePlan plan1 = new BaselinePlan("SELECT * FROM t2 WHERE id = ?", "SELECT * FROM t2 WHERE id = 1", 
                67890L, "SELECT * FROM t2 WHERE id = 1", 200.0);
        plan1.setId(GlobalStateMgr.getCurrentState().getNextId());
        plans.add(plan1);

        // 2. Mock EditLog.logCreateSPMBaseline to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateSPMBaseline(any(BaselinePlan.Info.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute storeBaselinePlan operation and expect exception
        // Note: storeBaselinePlan catches exceptions, so we verify the editlog was called
        // and that the exception is handled
        storage.storeBaselinePlan(plans);

        // verify master state unchanged
        Set<Long> masterIds = storage.getAllBaselineIds();
        Assertions.assertFalse(masterIds.contains(plan1.getId()));
    }

    @Test
    public void testDropBaselinePlanNormalCase() throws Exception {
        // 1. Prepare test data - store a plan first
        List<BaselinePlan> plans = new ArrayList<>();
        BaselinePlan plan1 = new BaselinePlan("SELECT * FROM t3 WHERE id = ?", "SELECT * FROM t3 WHERE id = 1", 
                11111L, "SELECT * FROM t3 WHERE id = 1", 300.0);
        plan1.setId(GlobalStateMgr.getCurrentState().getNextId());
        plans.add(plan1);
        storage.storeBaselinePlan(plans);
        Set<Long> storedIds = storage.getAllBaselineIds();
        Assertions.assertTrue(storedIds.contains(plan1.getId()));

        // 2. Prepare drop operation
        List<Long> baseLineIds = new ArrayList<>();
        baseLineIds.add(plan1.getId());

        // 3. Execute dropBaselinePlan operation (master side)
        storage.dropBaselinePlan(baseLineIds);
        Set<Long> masterIds = storage.getAllBaselineIds();
        Assertions.assertFalse(masterIds.contains(plan1.getId()));

        // 4. Test follower replay functionality
        SQLPlanGlobalStorage followerStorage = new SQLPlanGlobalStorage();
        
        // First replay the create operation to set up the follower state
        BaselinePlan.Info createReplayInfo = (BaselinePlan.Info) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_SPM_BASELINE_LOG);
        followerStorage.replayBaselinePlan(createReplayInfo, true);
        Set<Long> followerIds = followerStorage.getAllBaselineIds();
        Assertions.assertTrue(followerIds.contains(plan1.getId()));

        // Then replay the drop operation
        BaselinePlan.Info replayInfo = (BaselinePlan.Info) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_SPM_BASELINE_LOG);
        followerStorage.replayBaselinePlan(replayInfo, false);
        followerIds = followerStorage.getAllBaselineIds();

        // 5. Verify follower state is consistent with master
        Assertions.assertFalse(followerIds.contains(plan1.getId()));
    }

    @Test
    public void testDropBaselinePlanEditLogException() throws Exception {
        // 1. Prepare test data - store a plan first
        List<BaselinePlan> plans = new ArrayList<>();
        BaselinePlan plan1 = new BaselinePlan("SELECT * FROM t4 WHERE id = ?", "SELECT * FROM t4 WHERE id = 1", 
                22222L, "SELECT * FROM t4 WHERE id = 1", 400.0);
        plan1.setId(GlobalStateMgr.getCurrentState().getNextId());
        plans.add(plan1);
        storage.storeBaselinePlan(plans);

        // check stored
        Set<Long> storedIds = storage.getAllBaselineIds();
        Assertions.assertTrue(storedIds.contains(plan1.getId()));
        // 2. Prepare drop operation
        List<Long> baseLineIds = new ArrayList<>();
        baseLineIds.add(plan1.getId());

        // 3. Mock EditLog.logDropSPMBaseline to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropSPMBaseline(any(BaselinePlan.Info.class), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute dropBaselinePlan operation
        // Note: dropBaselinePlan catches exceptions, so we verify the editlog was called
        storage.dropBaselinePlan(baseLineIds);
        Set<Long> masterIds = storage.getAllBaselineIds();
        Assertions.assertTrue(masterIds.contains(plan1.getId()));
    }

    @Test
    public void testControlBaselinePlanNormalCase() throws Exception {
        // 1. Prepare test data - store a plan first
        List<BaselinePlan> plans = new ArrayList<>();
        BaselinePlan plan1 = new BaselinePlan("SELECT * FROM t5 WHERE id = ?", "SELECT * FROM t5 WHERE id = 1", 
                33333L, "SELECT * FROM t5 WHERE id = 1", 500.0);
        plan1.setId(GlobalStateMgr.getCurrentState().getNextId());
        plans.add(plan1);
        storage.storeBaselinePlan(plans);

        storage.getCache().put(plan1.getId(), new BaselinePlan(plan1.getId(), plan1.getBindSqlHash()));

        // 2. Prepare control operation
        List<Long> baseLineIds = new ArrayList<>();
        baseLineIds.add(plan1.getId());
        boolean isEnable = true;

        // 3. Execute controlBaselinePlan operation (master side)
        storage.controlBaselinePlan(isEnable, baseLineIds);
        // verify cache is cleaned
        Assertions.assertNull(storage.getCache().getIfPresent(plan1.getId()));

        // 4. Test follower replay functionality
        SQLPlanGlobalStorage followerStorage = new SQLPlanGlobalStorage();
        followerStorage.getCache().put(plan1.getId(), new BaselinePlan(plan1.getId(), plan1.getBindSqlHash()));
        
        // First replay the create operation to set up the follower state
        BaselinePlan.Info createReplayInfo = (BaselinePlan.Info) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_SPM_BASELINE_LOG);
        followerStorage.replayBaselinePlan(createReplayInfo, true);

        // Then replay the control operation
        BaselinePlan.Info replayInfo = (BaselinePlan.Info) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ENABLE_SPM_BASELINE_LOG);
        followerStorage.replayUpdateBaselinePlan(replayInfo, isEnable);

        // 5. Verify follower state is updated
        Assertions.assertNull(followerStorage.getCache().getIfPresent(plan1.getId()));
    }

    @Test
    public void testControlBaselinePlanEditLogException() throws Exception {
        // 1. Prepare test data - store a plan first
        List<BaselinePlan> plans = new ArrayList<>();
        BaselinePlan plan1 = new BaselinePlan("SELECT * FROM t6 WHERE id = ?", "SELECT * FROM t6 WHERE id = 1", 
                44444L, "SELECT * FROM t6 WHERE id = 1", 600.0);
        plan1.setId(GlobalStateMgr.getCurrentState().getNextId());
        plans.add(plan1);
        storage.storeBaselinePlan(plans);

        storage.getCache().put(plan1.getId(), new BaselinePlan(plan1.getId(), plan1.getBindSqlHash()));

        // 2. Prepare control operation
        List<Long> baseLineIds = new ArrayList<>();
        baseLineIds.add(plan1.getId());
        boolean isEnable = false;

        // 3. Mock EditLog.logUpdateSPMBaseline to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logUpdateSPMBaseline(any(BaselinePlan.Info.class), anyBoolean(), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute controlBaselinePlan operation
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            storage.controlBaselinePlan(isEnable, baseLineIds);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify follower state unchanged
        Assertions.assertNotNull(storage.getCache().getIfPresent(plan1.getId()));
    }
}