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

package com.starrocks.connector.statistics;

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorTableTriggerAnalyzeMgrTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testScheduleSurvivesQueueException() throws Exception {
        AtomicInteger queueScheduleCount = new AtomicInteger();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
        };
        new MockUp<ConnectorAnalyzeTaskQueue>() {
            @Mock
            public void schedulePendingTask() {
                queueScheduleCount.incrementAndGet();
                throw new RuntimeException("mock schedule failure");
            }
        };
        CountDownLatch dictUpdated = new CountDownLatch(1);
        new MockUp<StatisticExecutor>() {
            @Mock
            public void updateDictSync(String tableUUID, String columnName, Optional<String> fileName) {
                dictUpdated.countDown();
            }
        };

        ConnectorTableTriggerAnalyzeMgr mgr = new ConnectorTableTriggerAnalyzeMgr();
        mgr.addDictUpdateTask(new ConnectorTableColumnKey("table_uuid", "col"), Optional.of("file"));

        // the exception from queue scheduling must not escape, otherwise scheduleAtFixedRate stops forever,
        // and dict update must still be scheduled in the same round
        Assertions.assertDoesNotThrow(mgr::schedulePendingTask);
        Assertions.assertEquals(1, queueScheduleCount.get());
        Assertions.assertTrue(dictUpdated.await(30, TimeUnit.SECONDS));

        Assertions.assertDoesNotThrow(mgr::schedulePendingTask);
        Assertions.assertEquals(2, queueScheduleCount.get());
    }
}
