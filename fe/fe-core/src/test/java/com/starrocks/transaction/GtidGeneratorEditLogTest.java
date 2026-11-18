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

import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GtidInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
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

public class GtidGeneratorEditLogTest {

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testNextGtidWritesJournalAndFollowerInit() throws Exception {
        GtidGenerator leader = new GtidGenerator();
        long gtid = leader.nextGtid();
        Assertions.assertTrue(gtid > 0);
        Assertions.assertTrue(gtid <= leader.getBatchEndGtid());

        GtidInfo replayLog = (GtidInfo) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_SAVE_GTID);
        Assertions.assertEquals(leader.getBatchEndGtid(), replayLog.getBatchEndGtid());
        String json = GsonUtils.GSON.toJson(replayLog);
        Assertions.assertTrue(json.contains("\"bi\":"));

        GtidGenerator follower = new GtidGenerator();
        follower.init(replayLog.getBatchEndGtid());
        Assertions.assertEquals(leader.getBatchEndGtid(), follower.getBatchEndGtid());
        Assertions.assertEquals(leader.getBatchEndGtid(), follower.lastGtid());
    }

    @Test
    public void testNextGtidEditLogExceptionLeavesStateUnchanged() {
        GtidGenerator generator = new GtidGenerator();
        long batchEndGtid = generator.getBatchEndGtid();
        long lastTimestamp = generator.getLastTimestamp();
        long lastSequence = generator.getLastSequence();

        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logSaveGtid(anyLong(), any());
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, generator::nextGtid);
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed")
                || (exception.getCause() != null
                && exception.getCause().getMessage().contains("EditLog write failed")));

        Assertions.assertEquals(batchEndGtid, generator.getBatchEndGtid());
        Assertions.assertEquals(lastTimestamp, generator.getLastTimestamp());
        Assertions.assertEquals(lastSequence, generator.getLastSequence());
    }
}
