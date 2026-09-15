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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.lake.bookmark.AlreadyAtLatestException;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IvmMvPartitionPruningTest {
    private static final String DB = "db_ivm_mv_partition_pruning";
    private static final String FACT_DDL = "CREATE TABLE fact (dt DATE NOT NULL, k INT NOT NULL, v BIGINT) " +
            "DUPLICATE KEY(dt, k) PARTITION BY RANGE(dt) (" +
            "PARTITION p1 VALUES LESS THAN ('2024-02-01'), PARTITION p2 VALUES LESS THAN ('2024-03-01'), " +
            "PARTITION p3 VALUES LESS THAN ('2024-04-01'), PARTITION p4 VALUES LESS THAN ('2024-05-01')) " +
            "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')";
    private static final String DIM_DDL = "CREATE TABLE dim (k INT NOT NULL, w BIGINT) DUPLICATE KEY(k) " +
            "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')";
    // Two range partitions per calendar month, so a month-grained MV maps 2 base partitions to 1 of its own.
    private static final String ROLLUP_BASE_DDL = "CREATE TABLE rollup_fact (dt DATE NOT NULL, k INT NOT NULL, " +
            "v BIGINT) DUPLICATE KEY(dt, k) PARTITION BY RANGE(dt) (" +
            "PARTITION jan_a VALUES [('2024-01-01'), ('2024-01-15')), " +
            "PARTITION jan_b VALUES [('2024-01-15'), ('2024-02-01')), " +
            "PARTITION feb_a VALUES [('2024-02-01'), ('2024-02-15')), " +
            "PARTITION feb_b VALUES [('2024-02-15'), ('2024-03-01'))) " +
            "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')";
    // One range partition per calendar month, so a day-grained MV maps 1 base partition to many of its own.
    private static final String MONTH_BASE_DDL = "CREATE TABLE month_fact (dt DATE NOT NULL, k INT NOT NULL, " +
            "v BIGINT) DUPLICATE KEY(dt, k) PARTITION BY RANGE(dt) (" +
            "PARTITION m_jan VALUES [('2024-01-01'), ('2024-02-01')), " +
            "PARTITION m_feb VALUES [('2024-02-01'), ('2024-03-01'))) " +
            "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')";

    private static final long DEFAULT_MAX_ROWS_PER_REFRESH = Config.mv_max_rows_per_refresh;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static Database db;
    private final List<String> createdMvs = new ArrayList<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        starRocksAssert.withTable(FACT_DDL);
        starRocksAssert.withTable(DIM_DDL);
        starRocksAssert.withTable(ROLLUP_BASE_DDL);
        starRocksAssert.withTable(MONTH_BASE_DDL);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
    }

    @AfterEach
    public void after() throws Exception {
        Config.mv_max_rows_per_refresh = DEFAULT_MAX_ROWS_PER_REFRESH;
        for (String mv : createdMvs) {
            starRocksAssert.dropMaterializedView(mv);
        }
        createdMvs.clear();
    }

    @Test
    public void testMvScanBoundedToChangedBasePartitions() throws Exception {
        MaterializedView mv = createMv("mv_fact", "SELECT dt, k, SUM(v) AS total FROM fact GROUP BY dt, k");
        seedBaseline(mv, "fact");

        bumpVersion("fact", "p2");
        assertMvScanPartitions(explainRefresh(mv), "partitions=1/4");

        bumpVersion("fact", "p1");
        bumpVersion("fact", "p4");
        assertMvScanPartitions(explainRefresh(mv), "partitions=3/4");

        seedBaseline(mv, "fact");
        bumpVersion("fact", "p3");
        assertMvScanPartitions(explainRefresh(mv), "partitions=1/4");
    }

    @Test
    public void testNonRefBaseTableChangeKeepsFullScan() throws Exception {
        MaterializedView mv = createMv("mv_join",
                "SELECT f.dt, f.k, SUM(f.v * d.w) AS total FROM fact f JOIN dim d ON f.k = d.k GROUP BY f.dt, f.k");
        seedBaseline(mv, "fact");
        seedBaseline(mv, "dim");

        bumpVersion("fact", "p2");
        assertMvScanPartitions(explainRefresh(mv), "partitions=1/4");

        bumpVersion("dim", "dim");
        assertMvScanPartitions(explainRefresh(mv), "partitions=4/4");
    }

    /**
     * A self-join puts the same table in the partition-reference role and in another one. Its delta joins
     * rows of every partition through the second role, which the table-keyed topology cannot express.
     */
    @Test
    public void testSelfJoinKeepsWholeMvScan() throws Exception {
        MaterializedView mv = createMv("mv_self",
                "SELECT a.dt AS dt, a.k AS k, SUM(a.v * b.v) AS total "
                        + "FROM fact a JOIN fact b ON a.k = b.k GROUP BY a.dt, a.k");
        seedBaseline(mv, "fact");

        bumpVersion("fact", "p2");
        assertMvScanPartitions(explainRefresh(mv), "partitions=4/4");
    }

    @Test
    public void testPruningDisabledByMvProperty() throws Exception {
        MaterializedView mv = createMv("mv_off", "SELECT dt, k, SUM(v) AS total FROM fact GROUP BY dt, k",
                ", 'session.enable_ivm_mv_partition_pruning' = 'false'");
        seedBaseline(mv, "fact");
        bumpVersion("fact", "p2");

        assertMvScanPartitions(explainRefresh(mv), "partitions=4/4");
    }

    @Test
    public void testDeltaTouchingEveryPartitionScansWholeMv() throws Exception {
        MaterializedView mv = createMv("mv_all", "SELECT dt, k, SUM(v) AS total FROM fact GROUP BY dt, k");
        seedBaseline(mv, "fact");
        for (String partition : List.of("p1", "p2", "p3", "p4")) {
            bumpVersion("fact", partition);
        }
        assertMvScanPartitions(explainRefresh(mv), "partitions=4/4");
    }

    /**
     * Pruning is only sound while one run consumes the whole delta: a cloud-native range yields a single
     * delta trait, which computeAdaptiveDelta cannot truncate. Splitting it later would leave the prune
     * set at the head while the delta stopped short, and no other assertion would notice.
     */
    @Test
    public void testCloudNativeDeltaIsNeverSplitIntoBatches() throws Exception {
        MaterializedView mv = createMv("mv_batch", "SELECT dt, k, SUM(v) AS total FROM fact GROUP BY dt, k");
        seedBaseline(mv, "fact");
        bumpVersion("fact", "p2");

        Config.mv_max_rows_per_refresh = 1;
        MVIVMRefreshProcessor processor = planRefresh(mv);
        Assertions.assertFalse(processor.hasNextBatchRun(),
                "a cloud-native delta must be consumed in one run, otherwise the pruned MV scan can miss state");
    }

    /**
     * Roll-up shape: the MV is coarser than its base, so several base partitions map to one MV partition.
     * The scan must cover the union of the mapped MV partitions and nothing else.
     */
    @Test
    public void testRollupMvScansOnlyTheMappedCoarserPartitions() throws Exception {
        MaterializedView mv = createRollupMv("mv_rollup");
        seedBaseline(mv, "rollup_fact");

        bumpVersion("rollup_fact", "jan_a");
        assertMvScanPartitions(explainRefresh(mv), "partitions=1/2");

        bumpVersion("rollup_fact", "jan_b");
        assertMvScanPartitions(explainRefresh(mv), "partitions=1/2");

        bumpVersion("rollup_fact", "feb_a");
        assertMvScanPartitions(explainRefresh(mv), "partitions=2/2");
    }

    /**
     * The mirror image: the MV is finer than its base, so one base partition maps to every MV partition
     * inside its range. Under-scanning here would drop state for days the delta does reach.
     */
    @Test
    public void testFinerGrainedMvScansEveryMappedPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_daily " +
                "PARTITION BY date_trunc('day', dt) DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL') AS " +
                "SELECT dt, k, SUM(v) AS total FROM month_fact GROUP BY dt, k");
        createdMvs.add("mv_daily");
        MaterializedView mv = (MaterializedView) db.getTable("mv_daily");
        seedBaseline(mv, "month_fact");

        // January 2024 spans 31 of the MV's 60 day-partitions; February's 29 must stay out.
        bumpVersion("month_fact", "m_jan");
        assertMvScanPartitions(explainRefresh(mv), "partitions=31/60");

        bumpVersion("month_fact", "m_feb");
        assertMvScanPartitions(explainRefresh(mv), "partitions=60/60");
    }

    private MaterializedView createMv(String name, String query) throws Exception {
        return createMv(name, query, "");
    }

    private MaterializedView createRollupMv(String name) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name +
                " PARTITION BY date_trunc('month', dt) DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL') AS " +
                "SELECT dt, k, SUM(v) AS total FROM rollup_fact GROUP BY dt, k");
        createdMvs.add(name);
        return (MaterializedView) db.getTable(name);
    }

    private MaterializedView createMv(String name, String query, String extraProperties) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " PARTITION BY dt " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = 'INCREMENTAL'" + extraProperties + ") AS " + query);
        createdMvs.add(name);
        MaterializedView mv = (MaterializedView) db.getTable(name);
        Assertions.assertTrue(mv.isCloudNativeMaterializedView());
        return mv;
    }

    /** Pin the MV's consumed position on {@code tableName} at the table's current state. */
    private static void seedBaseline(MaterializedView mv, String tableName) throws Exception {
        OlapTable table = (OlapTable) db.getTable(tableName);
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        long bookmarkId;
        try {
            bookmarkId = bookmarkManager.create(db.getId(), table.getId(), BookmarkHolder.forMv(mv.getMvId()))
                    .getBookmarkId();
        } catch (AlreadyAtLatestException e) {
            bookmarkId = e.getBookmarkId();
        }
        BaseTableInfo baseTableInfo = mv.getBaseTableInfos().stream()
                .filter(info -> info.matchTable(table))
                .findFirst()
                .orElseThrow();
        mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap()
                .put(baseTableInfo, TvrTableSnapshot.of(bookmarkId));
    }

    private static void bumpVersion(String tableName, String partitionName) {
        OlapTable table = (OlapTable) db.getTable(tableName);
        PhysicalPartition partition = table.getPartition(partitionName).getDefaultPhysicalPartition();
        partition.setVisibleVersion(partition.getVisibleVersion() + 1, System.currentTimeMillis());
    }

    /** Plan through a real task run, so the caller can inspect the processor that planned it. */
    private static MVIVMRefreshProcessor planRefresh(MaterializedView mv) throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        StatementBase statement = MVTestBase.getAnalyzedPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName(), connectContext);
        ExecuteOption executeOption = new ExecuteOption(70, false, new HashMap<>());
        TaskRun taskRun = taskManager.buildTaskRun(task, executeOption);
        MVRefreshProcessor.ProcessExecPlan plan =
                taskManager.getMVRefreshProcessExecPlan(taskRun, task, executeOption, statement);
        Assertions.assertNotNull(plan.execPlan());
        assertMvScanPartitions(plan.execPlan().getExplainString(TExplainLevel.NORMAL), "partitions=1/4");
        MVTaskRunProcessor taskRunProcessor = (MVTaskRunProcessor) taskRun.getProcessor();
        return (MVIVMRefreshProcessor) taskRunProcessor.getMVRefreshProcessor();
    }

    private static String explainRefresh(MaterializedView mv) {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        Assertions.assertNotNull(task);
        StatementBase statement = MVTestBase.getAnalyzedPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW " + mv.getName(), connectContext);
        return taskManager.getMVRefreshExplain(task, new ExecuteOption(70, false, new HashMap<>()), statement);
    }

    private static void assertMvScanPartitions(String explain, String expected) {
        int scan = explain.indexOf("PREAGGREGATION");
        Assertions.assertTrue(scan >= 0, "no MV scan in plan:\n" + explain);
        String scanNode = explain.substring(scan, Math.min(explain.length(), scan + 300));
        Assertions.assertTrue(scanNode.contains(expected),
                "expected " + expected + " in MV scan node:\n" + scanNode + "\nfull plan:\n" + explain);
    }
}
