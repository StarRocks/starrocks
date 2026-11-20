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
import com.starrocks.common.tvr.TvrDeltaStats;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.MVTaskRunProcessor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.MVRefreshProcessor;
import com.starrocks.scheduler.mv.hybrid.MVHybridRefreshProcessor;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ExecPlan;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Drives refresh jobs of MVs over the mock Iceberg catalog batch by batch, with a snapshot id per base table, so a
 * subclass can move one base table and inspect the MV metadata between batches.
 */
abstract class IvmIcebergRefreshJobTestBase extends MVIVMIcebergTestBase {
    protected static final String FACT = "t1";
    protected static final String DIM = "t0";
    protected static final List<String> FACT_PARTITIONS =
            List.of("date=2020-01-01", "date=2020-01-02", "date=2020-01-03", "date=2020-01-04");

    private final Map<String, Long> snapshotIds = new HashMap<>();
    protected Runnable onWrite = () -> {
    };
    protected int writes = 0;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMIcebergTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
    }

    @BeforeEach
    public void mockBaseTablesAndWrites() {
        snapshotIds.put(FACT, 1L);
        snapshotIds.put(DIM, 1L);
        new MockUp<MockIcebergMetadata>() {
            @Mock
            public TvrTableSnapshot getCurrentTvrSnapshot(String dbName, com.starrocks.catalog.Table table) {
                return TvrTableSnapshot.of(Optional.of(currentSnapshotId(table.getName())));
            }

            @Mock
            public List<TvrTableDeltaTrait> listTableDeltaTraits(String dbName, com.starrocks.catalog.Table table,
                                                                 TvrTableSnapshot fromSnapshotExclusive,
                                                                 TvrTableSnapshot toSnapshotInclusive) {
                long from = fromSnapshotExclusive.isEmpty() ? 0L : fromSnapshotExclusive.getSnapshotId();
                List<TvrTableDeltaTrait> traits = new ArrayList<>();
                for (long v = from + 1; v <= toSnapshotInclusive.getSnapshotId(); v++) {
                    traits.add(TvrTableDeltaTrait.ofMonotonic(TvrTableDelta.of(v - 1, v), TvrDeltaStats.EMPTY));
                }
                return traits;
            }
        };
        new MockUp<MVTaskRunProcessor>() {
            @Mock
            public void executePlan(Invocation invocation, ExecPlan execPlan, InsertStmt insertStmt) {
                onWrite.run();
                writes++;
                invocation.proceed();
            }
        };
    }

    @Override
    public void advanceTableVersionTo(long toVersion) {
        snapshotIds.replaceAll((table, snapshotId) -> toVersion);
    }

    protected long currentSnapshotId(String table) {
        return snapshotIds.getOrDefault(table, 1L);
    }

    /** A new snapshot of the fact table that touches every partition. */
    protected void loadFact() {
        snapshotIds.merge(FACT, 1L, Long::sum);
        icebergMetadata().updatePartitions(MockIcebergMetadata.MOCKED_PARTITIONED_DB_NAME, FACT, FACT_PARTITIONS);
    }

    protected void loadDim() {
        snapshotIds.merge(DIM, 1L, Long::sum);
        icebergMetadata().updatePartitions(MockIcebergMetadata.MOCKED_UNPARTITIONED_DB_NAME, DIM, List.of(DIM));
    }

    private static MockIcebergMetadata icebergMetadata() {
        return (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr()
                .getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).orElseThrow();
    }

    protected MaterializedView createFactMv(String name, String refreshMode) throws Exception {
        return createMv(name, refreshMode, "SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1`");
    }

    /** Partitioned by the fact table, so the dim table is a non-ref base table. */
    protected MaterializedView createJoinMv(String name, String refreshMode) throws Exception {
        return createMv(name, refreshMode, "SELECT f.id, f.data, f.date, d.data AS dim_data " +
                "FROM `iceberg0`.`partitioned_db`.`t1` f JOIN `iceberg0`.`unpartitioned_db`.`t0` d ON f.id = d.id");
    }

    /** {@code auto} is created as incremental and switched over, as the DDL rejects it. */
    protected MaterializedView createMv(String name, String refreshMode, String query) throws Exception {
        boolean auto = "auto".equalsIgnoreCase(refreshMode);
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`" + name + "` " +
                "PARTITION BY `date` REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = '" + (auto ? "incremental" : refreshMode) + "', " +
                "'partition_refresh_number' = '1', 'partition_refresh_strategy' = 'strict') AS " + query);
        MaterializedView mv = getMv(name);
        if (auto) {
            mv.setCurrentRefreshMode(MaterializedView.RefreshMode.AUTO);
        }
        Assertions.assertEquals(MaterializedView.RefreshMode.valueOf(refreshMode.toUpperCase(Locale.ROOT)),
                mv.getCurrentRefreshMode());
        return mv;
    }

    protected static TaskRun runFirstBatch(MaterializedView mv) throws Exception {
        return runFirstBatch(mv, Map.of());
    }

    protected static TaskRun runForceFirstBatch(MaterializedView mv) throws Exception {
        return runFirstBatch(mv, Map.of(TaskRun.FORCE, "true"));
    }

    private static TaskRun runFirstBatch(MaterializedView mv, Map<String, String> requestProperties)
            throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(mv);
        if (task == null) {
            task = TaskBuilder.buildMvTask(mv, "test");
            taskManager.createTask(task);
        }
        task.getProperties().put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(requestProperties).build();
        executeBatch(taskRun);
        return taskRun;
    }

    protected static String executedRefreshMode(TaskRun taskRun) {
        return taskRun.getStatus().getMvTaskRunExtraMessage().getRefreshMode();
    }

    protected static Constants.TaskRunState executeBatch(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        return taskRun.executeTaskRun();
    }

    /** The batch {@code executed} spawned, or null when it was the job's last one. */
    protected static TaskRun nextOf(TaskRun executed) {
        MVRefreshProcessor processor = getMVTaskRunProcessor(executed).getMVRefreshProcessor();
        if (processor instanceof MVHybridRefreshProcessor hybrid) {
            return hybrid.getCurrentProcessor().getNextTaskRun();
        }
        return processor.getNextTaskRun();
    }

    protected static void runRemainingBatches(TaskRun firstBatch) throws Exception {
        for (TaskRun batch = nextOf(firstBatch); batch != null; batch = nextOf(batch)) {
            executeBatch(batch);
        }
    }

    protected static void runJob(MaterializedView mv) throws Exception {
        runRemainingBatches(runFirstBatch(mv));
    }

    protected static boolean runsAsPct(TaskRun taskRun) {
        MVRefreshProcessor processor = getMVTaskRunProcessor(taskRun).getMVRefreshProcessor();
        return ((MVHybridRefreshProcessor) processor).getCurrentProcessor() instanceof MVPCTRefreshProcessor;
    }

    protected static Map<BaseTableInfo, TvrVersionRange> tvrBaseline(MaterializedView mv) {
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
    }

    protected static Map<BaseTableInfo, TvrVersionRange> tvrBaselineIn(ChangeMaterializedViewRefreshSchemeLog log) {
        return log.getAsyncRefreshContext().getBaseTableInfoTvrVersionRangeMap();
    }

    protected static boolean hasBaseline(MaterializedView mv, String table) {
        return tvrBaseline(mv).keySet().stream().anyMatch(info -> info.getTableName().equals(table));
    }

    protected static long baselineSnapshotId(MaterializedView mv, String table) {
        return snapshotIdIn(tvrBaseline(mv), table);
    }

    protected static long frozenSnapshotId(MaterializedView mv, String table) {
        return snapshotIdIn(mv.getRefreshScheme().getAsyncRefreshContext().getTempBaseTableInfoTvrDeltaMap(), table);
    }

    private static long snapshotIdIn(Map<BaseTableInfo, TvrVersionRange> tvrMap, String table) {
        return tvrMap.entrySet().stream()
                .filter(entry -> entry.getKey().getTableName().equals(table))
                .findFirst().orElseThrow(() -> new AssertionError(table + " is not in " + tvrMap))
                .getValue().to().getVersion();
    }

    /** Every refresh scheme journal entry, as it was written: the in-memory scheme keeps changing afterwards. */
    protected static List<ChangeMaterializedViewRefreshSchemeLog> captureRefreshSchemeJournal() {
        List<ChangeMaterializedViewRefreshSchemeLog> journal = new ArrayList<>();
        new MockUp<EditLog>() {
            @Mock
            public void logMvChangeRefreshScheme(Invocation invocation, ChangeMaterializedViewRefreshSchemeLog log,
                                                 WALApplier walApplier) {
                journal.add(GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log),
                        ChangeMaterializedViewRefreshSchemeLog.class));
                invocation.proceed();
            }
        };
        return journal;
    }

    protected static void failIncrementalPlanning() {
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                throw new SemanticException("incremental planning failed (simulated)");
            }
        };
    }

    protected static void failIncrementalPlanningOnce() {
        boolean[] armed = {true};
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(Invocation invocation,
                                                                          TaskRunContext taskRunContext) {
                if (armed[0]) {
                    armed[0] = false;
                    throw new SemanticException("incremental planning failed (simulated)");
                }
                return invocation.proceed();
            }
        };
    }
}
