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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

/**
 * A batched pct rebuild leaves its remaining batches queued. When another complete refresh records the baseline before
 * they run, they belong to a superseded job and must be skipped, whatever the view's refresh mode.
 */
public class IvmSupersededPinnedBatchTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_superseded_pinned_batch";

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testQueuedBatchIsSkippedOnceAnotherRefreshRecordsTheBaseline(String refreshMode) throws Exception {
        OlapTable fact = createFact("fact_superseded_" + refreshMode);
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_superseded_" + refreshMode, fact, refreshMode);
        mockLakeDml();
        TaskRun queued = nextOf(runFirstBatch(mv));
        Assertions.assertNotNull(queued, "the rebuild was not batched");
        Assertions.assertNotNull(queued.getProperties().get(TaskRun.PINNED_REFRESH_JOB_ID));

        runJob(mv, fact);
        Map<BaseTableInfo, TvrVersionRange> baseline = new HashMap<>(tvrBaseline(mv));
        Assertions.assertFalse(baseline.isEmpty());

        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(queued));
        Assertions.assertInstanceOf(MVHybridRefreshProcessor.class, refreshProcessorOf(queued));
        Assertions.assertEquals(baseline, tvrBaseline(mv));
        Assertions.assertNull(nextOf(queued));
    }
}
