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
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * REFRESH ... FORCE on an incrementally maintained MV rebuilds it from one frozen snapshot and sets every base table's
 * baseline to that snapshot, so the refreshes after it are incremental again.
 */
public class IvmForceRefreshTest extends IvmIcebergRefreshJobTestBase {

    @Test
    public void testForceOnAnIncrementalViewRebuildsInsteadOfApplyingTheDelta() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_force_routing", "incremental");
        runJob(mv);
        loadFact();

        TaskRun force = runForceFirstBatch(mv);
        Assertions.assertInstanceOf(MVHybridRefreshProcessor.class,
                getMVTaskRunProcessor(force).getMVRefreshProcessor());
        Assertions.assertEquals("PCT", executedRefreshMode(force));
    }

    @ParameterizedTest
    @ValueSource(strings = {"incremental", "auto"})
    public void testForceRewritesEveryPartitionOfAnUnchangedView(String refreshMode) throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_force_unchanged_" + refreshMode, refreshMode);
        runJob(mv);

        TaskRun batch = runForceFirstBatch(mv);
        Set<String> rewritten = new TreeSet<>(refreshedMvPartitions(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
            rewritten.addAll(refreshedMvPartitions(batch));
        }
        Assertions.assertEquals(mv.getPartitionNames(), rewritten, "FORCE rewrites partitions that did not change");
        Assertions.assertEquals(currentSnapshotId(FACT), baselineSnapshotId(mv, FACT));

        loadFact();
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(runFirstBatch(mv)));
    }

    @Test
    public void testForceDropsTheBaselineOfAnUnchangedTableBeforeItsFirstWrite() throws Exception {
        loadFact();
        loadDim();
        MaterializedView mv = createJoinMv("mv_force_unchanged_dim", "incremental");
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        boolean[] forcing = {false};
        List<Boolean> journaledEmptyAtWrite = new ArrayList<>();
        onWrite = () -> {
            if (forcing[0]) {
                journaledEmptyAtWrite.add(journal.stream().anyMatch(log -> tvrBaselineIn(log).isEmpty()));
            }
        };
        runJob(mv);
        loadFact();

        journal.clear();
        forcing[0] = true;
        TaskRun batch = runForceFirstBatch(mv);
        Assertions.assertEquals(List.of(true), journaledEmptyAtWrite,
                "the rebuild does not trust the dim table's baseline either, so it must leave the journal first");
        runRemainingBatches(batch);
        Assertions.assertEquals(Set.of(FACT, DIM),
                tvrBaseline(mv).keySet().stream().map(BaseTableInfo::getTableName).collect(Collectors.toSet()));
        Assertions.assertEquals(currentSnapshotId(DIM), baselineSnapshotId(mv, DIM));
    }

    @Test
    public void testInterruptedForceOfAnUnchangedViewIsFinishedByTheNextRefresh() throws Exception {
        loadFact();
        MaterializedView mv = createFactMv("mv_force_interrupted", "incremental");
        runJob(mv);

        TaskRun force = runForceFirstBatch(mv);
        Assertions.assertNotNull(nextOf(force), "partition_refresh_number=1 must split the rebuild into batches");
        Set<String> notRewritten = new TreeSet<>(mv.getPartitionNames());
        notRewritten.removeAll(refreshedMvPartitions(force));

        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch),
                "a rebuild that stopped part way must be finished, not followed by an empty incremental run");
        Set<String> finished = new TreeSet<>(refreshedMvPartitions(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
            finished.addAll(refreshedMvPartitions(batch));
        }
        Assertions.assertEquals(notRewritten, finished);
        Assertions.assertEquals(currentSnapshotId(FACT), baselineSnapshotId(mv, FACT));
    }

    private static Set<String> refreshedMvPartitions(TaskRun batch) {
        return batch.getStatus().getMvTaskRunExtraMessage().getMvPartitionsToRefresh();
    }
}
