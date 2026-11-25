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

import com.starrocks.common.FeConstants;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadsHistorySyncerTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    // what the syncer told the stream load manager and the load manager about loads_history
    private final List<Long> syncedBeforeMs = new ArrayList<>();
    private final List<Long> loadJobsSyncedBeforeMs = new ArrayList<>();

    @BeforeEach
    public void setUp() {
        syncedBeforeMs.clear();
        loadJobsSyncedBeforeMs.clear();
        StreamLoadMgr streamLoadMgr = new StreamLoadMgr();
        LoadMgr loadMgr = new LoadMgr(new LoadJobScheduler());
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getStreamLoadMgr();
                minTimes = 0;
                result = streamLoadMgr;

                globalStateMgr.getLoadMgr();
                minTimes = 0;
                result = loadMgr;
            }
        };
        new MockUp<StreamLoadMgr>() {
            @Mock
            public void setLoadsHistorySyncedBeforeMs(long timeMs) {
                syncedBeforeMs.add(timeMs);
            }
        };
        new MockUp<LoadMgr>() {
            @Mock
            public void setLoadsHistorySyncedBeforeMs(long timeMs) {
                loadJobsSyncedBeforeMs.add(timeMs);
            }
        };
    }

    @Test
    public void testOnlyASuccessfulSyncTellsTheStreamLoadManagerWhatLoadsHistoryHas() {
        AtomicBoolean failSync = new AtomicBoolean(true);
        AtomicInteger syncs = new AtomicInteger();
        new MockUp<SimpleExecutor>() {
            @Mock
            public void executeDML(String sql) {
                syncs.incrementAndGet();
                if (failSync.get()) {
                    throw new SemanticException("injected sync failure");
                }
            }
        };
        LoadsHistorySyncer syncer = new LoadsHistorySyncer();
        long latestFinishTime = System.currentTimeMillis() - 300_000L;

        // a failed sync copied nothing, so the manager keeps its tasks and the next round syncs again
        syncer.syncIfNewLoadsFinished(latestFinishTime);
        Assertions.assertEquals(1, syncs.get());
        Assertions.assertEquals(List.of(), syncedBeforeMs);
        Assertions.assertEquals(List.of(), loadJobsSyncedBeforeMs);

        failSync.set(false);
        long beforeSyncMs = System.currentTimeMillis();
        syncer.syncIfNewLoadsFinished(latestFinishTime);
        long afterSyncMs = System.currentTimeMillis();
        Assertions.assertEquals(2, syncs.get());
        Assertions.assertEquals(1, syncedBeforeMs.size());
        // the sync copied the loads that finished more than a minute before it, less the clock skew margin
        Assertions.assertTrue(syncedBeforeMs.get(0) >= beforeSyncMs - 70_000L, syncedBeforeMs.toString());
        Assertions.assertTrue(syncedBeforeMs.get(0) <= afterSyncMs - 70_000L, syncedBeforeMs.toString());
        Assertions.assertEquals(syncedBeforeMs, loadJobsSyncedBeforeMs);

        // nothing finished since
        syncer.syncIfNewLoadsFinished(latestFinishTime);
        Assertions.assertEquals(2, syncs.get());
    }

    @Test
    public void testTheManagerKeepsTasksOnlyWhileThisFeSyncsLoadsHistory() {
        LoadsHistorySyncer syncer = new LoadsHistorySyncer();
        boolean savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        try {
            // the warm-up round of a new leader
            syncer.runAfterLeaseValid();
        } finally {
            FeConstants.runningUnitTest = savedRunningUnitTest;
        }
        Assertions.assertEquals(List.of(0L), syncedBeforeMs);
        syncer.onStopped();
        Assertions.assertEquals(List.of(0L, Long.MAX_VALUE), syncedBeforeMs);
        Assertions.assertEquals(List.of(0L, Long.MAX_VALUE), loadJobsSyncedBeforeMs);
    }
}
