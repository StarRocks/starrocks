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

package com.starrocks.transaction;

import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.TransactionIdInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class TransactionIdGeneratorEditLogTest {
    private TransactionIdGenerator idGenerator;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create TransactionIdGenerator instance
        idGenerator = new TransactionIdGenerator();
        // Initialize to near batch end to trigger editlog write
        idGenerator.initTransactionId(999L);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testGetNextTransactionIdNormalCase() throws Exception {
        // 1. Verify initial state
        // After initTransactionId(999L), nextId should be 999, batchEndId should be 999
        // When getNextTransactionId is called, nextId (999) >= batchEndId (999), so it will trigger editlog write
        // newBatchEndId = 999 + 1000 = 1999, then nextId++ returns 1000
        
        // 2. Execute getNextTransactionId operation (master side) - this should trigger editlog write
        long nextId = idGenerator.getNextTransactionId();

        // 3. Verify master state
        Assertions.assertEquals(1000L, nextId);
        // batchEndId should be updated to 1999
        long batchEndId = idGenerator.getBatchEndId();
        Assertions.assertEquals(1999L, batchEndId);

        // 4. Test follower replay functionality
        TransactionIdGenerator followerIdGenerator = new TransactionIdGenerator();
        followerIdGenerator.initTransactionId(999L);
        
        // Verify follower initial state
        long followerBatchEndId = followerIdGenerator.getBatchEndId();
        Assertions.assertEquals(999L, followerBatchEndId);

        // Replay the operation
        TransactionIdInfo replayLog = (TransactionIdInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SAVE_TRANSACTION_ID_V2);
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                .initTransactionId(replayLog.getTxnId() + 1);
        followerIdGenerator.initTransactionId(replayLog.getTxnId());

        // 5. Verify follower state is consistent with master
        long replayedBatchEndId = followerIdGenerator.getBatchEndId();
        Assertions.assertEquals(1999L, replayedBatchEndId);
        Assertions.assertEquals(batchEndId, replayedBatchEndId);
        
        // Verify the transaction ID info
        Assertions.assertEquals(1999L, replayLog.getTxnId());
    }

    @Test
    public void testGetNextTransactionIdEditLogException() {
        // 1. Verify initial state
        long initialBatchEndId = idGenerator.getBatchEndId();
        Assertions.assertEquals(999L, initialBatchEndId);

        // 2. Mock EditLog.logSaveTransactionId to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logSaveTransactionId(anyLong(), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute getNextTransactionId operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            idGenerator.getNextTransactionId();
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") || 
                            exception.getCause() != null && 
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 4. Verify leader memory state remains unchanged after exception
        // The batchEndId should not have been updated
        long currentBatchEndId = idGenerator.getBatchEndId();
        Assertions.assertEquals(initialBatchEndId, currentBatchEndId);
    }
}

