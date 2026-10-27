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
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.TaskRun;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * REFRESH ... FORCE on an incrementally maintained MV rebuilds it from one frozen snapshot and sets every base table's
 * baseline to that snapshot, so the refreshes after it are incremental again.
 */
public class IvmForceRefreshTest extends IvmCloudNativeRefreshTestBase {
    private static final String DB = "db_ivm_force_refresh";

    @BeforeAll
    public static void beforeClass() throws Exception {
        setUpSharedDataCluster(DB);
    }

    @Test
    public void testForceOnAnIncrementalViewRebuildsInsteadOfApplyingTheDelta() throws Exception {
        OlapTable fact = createFact("fact_force_routing");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_force_routing", fact, "INCREMENTAL");
        mockLakeDml();
        runJob(mv, fact);
        loadEveryPartition(fact);

        Assertions.assertEquals("PCT", executedRefreshMode(runForceFirstBatch(mv)));
    }

    @Test
    public void testForceRewritesEveryPartitionOfAnUnchangedIncrementalView() throws Exception {
        assertForceRewritesEveryPartitionOfAnUnchangedView("INCREMENTAL");
    }

    @Test
    public void testForceRewritesEveryPartitionOfAnUnchangedAutoView() throws Exception {
        assertForceRewritesEveryPartitionOfAnUnchangedView("AUTO");
    }

    private static void assertForceRewritesEveryPartitionOfAnUnchangedView(String refreshMode) throws Exception {
        String suffix = refreshMode.toLowerCase(Locale.ROOT);
        OlapTable fact = createFact("fact_force_unchanged_" + suffix);
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_force_unchanged_" + suffix, fact, refreshMode);
        mockLakeDml();
        runJob(mv, fact);

        TaskRun batch = runForceFirstBatch(mv);
        Assertions.assertEquals(MaterializedView.RefreshModeReason.FORCE_REFRESH.name(),
                batch.getStatus().getMvTaskRunExtraMessage().getRefreshModeReason());
        Set<String> rewritten = new TreeSet<>(refreshedBasePartitions(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
            rewritten.addAll(refreshedBasePartitions(batch));
        }
        Assertions.assertEquals(new TreeSet<>(PARTITIONS), rewritten, "FORCE rewrites partitions that did not change");
        Assertions.assertEquals(MaterializedView.RefreshModeReason.FORCE_REFRESH,
                mv.getRefreshScheme().getLastRefreshModeReason());
        assertRecordsMatchBaseline(mv, fact);

        loadPartition(fact, "p1");
        Assertions.assertEquals("INCREMENTAL", executedRefreshMode(runFirstBatch(mv)));
    }

    @Test
    public void testForceDropsTheBaselineOfAnUnchangedTableBeforeItsFirstWrite() throws Exception {
        OlapTable fact = createFact("fact_force_unchanged_dim");
        OlapTable dim = createDim("dim_force_unchanged_dim");
        loadEveryPartition(fact);
        loadPartition(dim, dim.getName());
        MaterializedView mv = createJoinMv("mv_force_unchanged_dim", fact, dim, "INCREMENTAL");
        List<ChangeMaterializedViewRefreshSchemeLog> journal = captureRefreshSchemeJournal();
        boolean[] forcing = {false};
        List<Boolean> journaledEmptyAtWrite = new ArrayList<>();
        mockLakeDml(insert -> {
            if (forcing[0]) {
                journaledEmptyAtWrite.add(journal.stream().anyMatch(log -> tvrBaselineIn(log).isEmpty()));
            }
        });
        runJob(mv, fact);
        loadEveryPartition(fact);

        journal.clear();
        forcing[0] = true;
        TaskRun batch = runForceFirstBatch(mv);
        Assertions.assertEquals(List.of(true), journaledEmptyAtWrite,
                "the rebuild does not trust the dim table's baseline either, so it must leave the journal first");
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
        Assertions.assertEquals(Set.of(fact.getId(), dim.getId()),
                tvrBaseline(mv).keySet().stream().map(BaseTableInfo::getTableId).collect(Collectors.toSet()));
    }

    @Test
    public void testInterruptedForceOfAnUnchangedViewIsFinishedByTheNextRefresh() throws Exception {
        OlapTable fact = createFact("fact_force_interrupted");
        loadEveryPartition(fact);
        MaterializedView mv = createMv("mv_force_interrupted", fact, "INCREMENTAL");
        mockLakeDml();
        runJob(mv, fact);

        TaskRun force = runForceFirstBatch(mv);
        Assertions.assertNotNull(nextOf(force), "partition_refresh_number=1 must split the rebuild into batches");
        Set<String> notRewritten = new TreeSet<>(PARTITIONS);
        notRewritten.removeAll(refreshedBasePartitions(force));

        TaskRun batch = runFirstBatch(mv);
        Assertions.assertEquals("PCT", executedRefreshMode(batch),
                "a rebuild that stopped part way must be finished, not followed by an empty incremental run");
        Set<String> finished = new TreeSet<>(refreshedBasePartitions(batch));
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
            finished.addAll(refreshedBasePartitions(batch));
        }
        Assertions.assertEquals(notRewritten, finished);
        assertRecordsMatchBaseline(mv, fact);
    }

    @Test
    public void testAnUnknownRefreshModeReasonReadsAsNull() {
        MaterializedView.MvRefreshScheme scheme = new MaterializedView.MvRefreshScheme();
        scheme.setLastRefreshModeReason(MaterializedView.RefreshModeReason.FORCE_REFRESH, null);
        String json = GsonUtils.GSON.toJson(scheme);
        Assertions.assertTrue(json.contains(MaterializedView.RefreshModeReason.FORCE_REFRESH.name()), json);

        MaterializedView.MvRefreshScheme read = GsonUtils.GSON.fromJson(
                json.replace(MaterializedView.RefreshModeReason.FORCE_REFRESH.name(), "A_REASON_THIS_VERSION_LACKS"),
                MaterializedView.MvRefreshScheme.class);
        Assertions.assertNull(read.getLastRefreshModeReason(),
                "a frontend rolled back past a new reason must still load the view");
    }
}
