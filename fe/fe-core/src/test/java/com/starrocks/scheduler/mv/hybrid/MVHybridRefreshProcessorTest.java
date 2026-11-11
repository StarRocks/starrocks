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

package com.starrocks.scheduler.mv.hybrid;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.tvr.TvrDeltaStats;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.ivm.MVIVMIcebergTestBase;
import com.starrocks.scheduler.mv.ivm.MVIVMRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellWithName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A PCT attempt can apply a partition shape change (drop the populated partitions, create their
 * replacements) and then fail before the insert commits. Within the same task run, the retry reuses the
 * same {@link MvTaskRunContext}, so the pending replacement partitions are still there and IVM cannot
 * materialize them: the retry must keep using PCT.
 */
public class MVHybridRefreshProcessorTest extends MVIVMIcebergTestBase {

    private static final String ICEBERG_T1 = "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`";

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
    }

    @Test
    public void testPendingReplacementPartitionsKeepPctRefresh() throws Exception {
        MaterializedView mv = createIncrementallyRefreshableMv();

        MVHybridRefreshProcessor hybrid = prepareHybridProcessor(mv);
        MvTaskRunContext mvContext = hybrid.getMvContext();
        // A previous attempt in this task run already replaced partitions and failed before the insert.
        mvContext.getReplacementAddPartitions().add(PCellWithName.of("p20220101", new PCellNone()));

        hybrid.getProcessExecPlan(mvContext);

        Assertions.assertInstanceOf(MVPCTRefreshProcessor.class, hybrid.getCurrentProcessor(),
                "IVM cannot materialize the replacement partitions, so the refresh must stay on PCT");
        Assertions.assertEquals(MaterializedView.RefreshMode.PCT.name(),
                mvContext.getStatus().getMvTaskRunExtraMessage().getRefreshMode(),
                "the task run status must report the PCT fallback");
    }

    @Test
    public void testRefreshWithoutReplacementPartitionsPlansThroughIvm() throws Exception {
        MaterializedView mv = createIncrementallyRefreshableMv();

        MVHybridRefreshProcessor hybrid = prepareHybridProcessor(mv);
        hybrid.getProcessExecPlan(hybrid.getMvContext());

        Assertions.assertInstanceOf(MVIVMRefreshProcessor.class, hybrid.getCurrentProcessor(),
                "without pending replacement partitions the same view refreshes incrementally");
    }

    /**
     * An auto-refresh MV over an append-only iceberg delta: IVM can plan it, so the hybrid refresh would
     * normally pick the IVM path.
     */
    private MaterializedView createIncrementallyRefreshableMv() throws Exception {
        MaterializedView mv = createMaterializedViewWithRefreshMode(ICEBERG_T1, "auto", "`date`", null);
        seedTvrBaselineAtVersionZero(mv);
        advanceTableVersionTo(2);
        mockListTableDeltaTraits(ImmutableList.of(
                TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(0L, 2L), TvrDeltaStats.EMPTY)));
        return mv;
    }

    /**
     * Prepare a refresh processor exactly like {@link MVTaskRunProcessor#processTaskRun} does, but stop
     * before planning so the test can arrange the task run context first.
     */
    private MVHybridRefreshProcessor prepareHybridProcessor(MaterializedView mv) throws Exception {
        TaskRun taskRun = buildMVTaskRun(mv, "test");
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        MVTaskRunProcessor processor = new MVTaskRunProcessor();
        processor.prepare(taskRun.buildTaskRunContext());
        Assertions.assertInstanceOf(MVHybridRefreshProcessor.class, processor.getMVRefreshProcessor());
        return (MVHybridRefreshProcessor) processor.getMVRefreshProcessor();
    }
}
