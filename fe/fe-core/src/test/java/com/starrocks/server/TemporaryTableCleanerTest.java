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

package com.starrocks.server;

import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import mockit.Mock;
import mockit.MockUp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class TemporaryTableCleanerTest {
    private static final Logger LOG = LogManager.getLogger(TemporaryTableCleanerTest.class);
    private static TemporaryTableCleaner cleaner;
    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.temporaryTableCleanerTest = true;
        cleaner = new TemporaryTableCleaner();
        GlobalStateMgr.getCurrentState().getTemporaryTableMgr().clear();
    }

    @Test
    public void testNormal() throws Exception {
        // normal case, the timing is as follows:
        // T1: TemporaryTableMgr only contains session1 and session2
        // T2: the alive sessions obtained by cleaner only includes session1
        // T3. after cleaner.runAfterCatalogReady, session2 is cleaned up.
        UUID session1 = UUIDUtil.genUUID();
        UUID session2 = UUIDUtil.genUUID();

        TemporaryTableMgr mgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        mgr.addTemporaryTable(session1, 1, "t1", 1);
        mgr.addTemporaryTable(session2, 2, "t2", 2);
        new MockUp<TemporaryTableCleaner>() {
            @Mock
            protected Set<UUID> getAliveSessions() throws Exception {
                Set<UUID> result = new HashSet<>();
                result.add(session1);
                return result;
            }
        };
        cleaner.runAfterCatalogReady();
        Assert.assertTrue(mgr.sessionExists(session1));
        Assert.assertFalse(mgr.sessionExists(session2));
    }

    @Test
    public void testAddSessionDuringCleanUp() throws Exception {
        // test the behavior of adding a new session during session cleanup
        // the timing is as follows:
        // T1: TemporaryTableMgr only contains session1
        // T2: the first invoke of cleaner.runAfterCatalogReady
        //   T2.1: getAliveSessions and there are no alive sessions
        //   T2.2: add the new session2 into TemporaryTableMgr
        //   T2.3: cleanup inactive sessions, session1 will be cleaned up and
        //         session2 should not be cleaned up at this time because it was added during cleanup task execution.
        // T3: the second invoke of cleaner.runAfterCatalogReady, all sessions will be cleaned up.
        UUID session1 = UUIDUtil.genUUID();
        TemporaryTableMgr mgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
        mgr.addTemporaryTable(session1, 1, "t1", 1);

        UUID session2 = UUIDUtil.genUUID();
        new MockUp<TemporaryTableCleaner>() {
            private int callCount = 0;
            @Mock
            protected Set<UUID> getAliveSessions() throws Exception {
                Set<UUID> result = new HashSet<>();
                return result;
            }

            @Mock
            protected void testMock() {
                ++callCount;
                if (callCount == 1) {
                    mgr.addTemporaryTable(session2, 2, "t2", 2);
                }
            }
        };
        cleaner.runAfterCatalogReady();
        Assert.assertFalse(mgr.sessionExists(session1));
        Assert.assertTrue(mgr.sessionExists(session2));

        cleaner.runAfterCatalogReady();
        Assert.assertFalse(mgr.sessionExists(session2));
    }
}
