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
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

/**
 * A batched pct rebuild leaves its remaining batches queued. When another complete refresh records the baseline before
 * they run, they belong to a superseded job and must be skipped, whatever the view's refresh mode.
 */
public class IvmSupersededPinnedBatchTest extends MVIVMIcebergTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testQueuedBatchIsSkippedOnceAnotherRefreshRecordsTheBaseline(String refreshMode) throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(
                "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`", refreshMode, "`date`",
                Map.of("partition_refresh_number", "1"));
        advanceTableVersionTo(2);
        mockListTableDeltaTraits();
        TaskRun queued = nextOf(withMVRefreshTaskRun("test", mv));
        Assertions.assertNotNull(queued, "the rebuild was not batched");
        Assertions.assertNotNull(queued.getProperties().get(TaskRun.PINNED_REFRESH_JOB_ID));

        for (TaskRun batch = nextOf(withMVRefreshTaskRun("test", mv)); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        Map<BaseTableInfo, TvrVersionRange> baseline = Map.copyOf(tvrBaseline(mv));
        Assertions.assertFalse(baseline.isEmpty());

        // Without this, a queued batch run unpinned would find its partitions fresh and skip for that reason.
        MockIcebergMetadata metadata = (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr()
                .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).orElseThrow();
        metadata.updatePartitions("partitioned_db", "t1", metadata.listPartitionNames("partitioned_db", "t1", null));

        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, executeBatch(queued));
        Assertions.assertInstanceOf(MVHybridRefreshProcessor.class,
                getMVTaskRunProcessor(queued).getMVRefreshProcessor());
        Assertions.assertEquals(baseline, tvrBaseline(mv));
        Assertions.assertNull(nextOf(queued));
    }

    private static Constants.TaskRunState executeBatch(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        return taskRun.executeTaskRun();
    }

    private static TaskRun nextOf(TaskRun executed) {
        MVRefreshProcessor processor = getMVTaskRunProcessor(executed).getMVRefreshProcessor();
        if (processor instanceof MVHybridRefreshProcessor hybrid) {
            return hybrid.getCurrentProcessor().getNextTaskRun();
        }
        return processor.getNextTaskRun();
    }

    private static Map<BaseTableInfo, TvrVersionRange> tvrBaseline(MaterializedView mv) {
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
    }
}
