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

package com.starrocks.catalog;

import com.starrocks.persist.EditLog;
import com.starrocks.persist.NextIdLog;
import com.starrocks.persist.OperationType;
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

public class CatalogIdGeneratorEditLogTest {
    private CatalogIdGenerator idGenerator;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create CatalogIdGenerator instance with initial value
        idGenerator = new CatalogIdGenerator(0L);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testGetNextIdNormalCase() throws Exception {
        // 1. Verify initial state
        long initialBatchEndId = idGenerator.getBatchEndId();
        Assertions.assertEquals(0L, initialBatchEndId);

        // 2. Get IDs until we trigger a batch update (should happen after 1000 IDs)
        // First, set the generator to near the batch end to trigger editlog write
        idGenerator.setId(999L);
        
        // 3. Execute getNextId operation (master side) - this should trigger editlog write
        // When nextId (999) >= batchEndId (999), it will trigger editlog write
        // newBatchEndId = 999 + 1000 = 1999, then return nextId++ which is 999
        long nextId = idGenerator.getNextId();

        // 4. Verify master state
        Assertions.assertEquals(999L, nextId);
        Assertions.assertEquals(1999L, idGenerator.getBatchEndId());

        // 5. Test follower replay functionality
        CatalogIdGenerator followerIdGenerator = new CatalogIdGenerator(0L);
        followerIdGenerator.setId(999L);
        
        // Verify follower initial state
        Assertions.assertEquals(999L, followerIdGenerator.getBatchEndId());

        // Replay the operation
        NextIdLog replayLog = (NextIdLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SAVE_NEXTID_V2);
        GlobalStateMgr.getCurrentState().setNextId(replayLog.getId() + 1);
        followerIdGenerator.setId(replayLog.getId());

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerIdGenerator.getBatchEndId() >= 1000L);
    }

    @Test
    public void testGetNextIdEditLogException() throws Exception {
        // 1. Set the generator to near the batch end to trigger editlog write
        idGenerator.setId(999L);
        
        long initialBatchEndId = idGenerator.getBatchEndId();
        Assertions.assertEquals(999L, initialBatchEndId);

        // 2. Mock EditLog.logSaveNextId to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logSaveNextId(anyLong(), any());
        
        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state snapshot
        long initialId = initialBatchEndId;

        // 3. Execute getNextId operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            idGenerator.getNextId();
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        // The batchEndId should not have been updated
        Assertions.assertEquals(initialId, idGenerator.getBatchEndId());
    }
}


