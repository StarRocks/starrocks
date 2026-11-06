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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.PhysicalPartitionMeta;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Drives refresh jobs of MVs over cloud-native tables batch by batch on a shared-data cluster, with the DML faked:
 * a subclass creates its tables and MVs through the helpers here and inspects the MV metadata between batches.
 */
abstract class IvmCloudNativeRefreshTestBase {
    protected static final List<String> PARTITIONS = List.of("p1", "p2", "p3", "p4");

    protected static StarRocksAssert starRocksAssert;
    protected static Database db;
    // One entry per DML: base table name -> (physical partition id -> version the scan read).
    protected static final List<Map<String, Map<Long, Long>>> SCANNED_VERSIONS = new ArrayList<>();

    protected static void setUpSharedDataCluster(String dbName) throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
    }

    /** Runs a refresh job to its last batch. Returns partition -> version the job scanned. */
    protected static Map<String, Long> runJob(MaterializedView mv, OlapTable fact) throws Exception {
        Map<String, Long> scanned = new TreeMap<>();
        int scansBefore = SCANNED_VERSIONS.size();
        TaskRun batch = runFirstBatch(mv);
        collectScannedVersions(batch, fact, scansBefore, scanned);
        for (batch = nextOf(batch); batch != null; batch = nextOf(batch)) {
            scansBefore = SCANNED_VERSIONS.size();
            executeBatch(batch);
            collectScannedVersions(batch, fact, scansBefore, scanned);
        }
        return scanned;
    }

    protected static void collectScannedVersions(TaskRun batch, OlapTable fact, int scansBefore,
                                                 Map<String, Long> scanned) {
        if (SCANNED_VERSIONS.size() == scansBefore) {
            return;
        }
        for (String name : refreshedBasePartitions(batch)) {
            scanned.put(name, scannedVersion(scansBefore, fact, name));
        }
    }

    protected static long scannedVersion(int scanIndex, OlapTable fact, String name) {
        Map<Long, Long> versions = SCANNED_VERSIONS.get(scanIndex).get(fact.getName());
        Assertions.assertNotNull(versions, "no scan of " + fact.getName() + ": " + SCANNED_VERSIONS.get(scanIndex));
        return versions.get(fact.getPartition(name).getDefaultPhysicalPartition().getId());
    }

    protected static Set<String> refreshedBasePartitions(TaskRun batch) {
        Set<String> partitions = new TreeSet<>();
        batch.getStatus().getMvTaskRunExtraMessage().getRefBasePartitionsToRefreshMap().values()
                .forEach(partitions::addAll);
        return partitions;
    }

    /** With a baseline in place, no partition may be recorded past the version its bookmark captured. */
    protected static void assertVersionMapNotAheadOfBaseline(MaterializedView mv, OlapTable fact) {
        long baselineId = bookmarkIdOf(mv.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap(), fact);
        for (String name : PARTITIONS) {
            Assertions.assertTrue(recordedVersion(mv, fact, name) <= partitionVersionIn(fact, baselineId, name),
                    name + " is recorded past the baseline");
        }
    }

    protected static void assertRecordsMatchBaseline(MaterializedView mv, OlapTable fact) {
        long baselineId = bookmarkIdOf(mv.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap(), fact);
        Bookmark baseline = GlobalStateMgr.getCurrentState().getBookmarkManager()
                .findBookmarkById(db.getId(), fact.getId(), baselineId).orElseThrow();
        for (String name : PARTITIONS) {
            Partition partition = fact.getPartition(name);
            PhysicalPartitionMeta captured = baseline.getPhysicalPartitionMeta(partition.getId(),
                    partition.getLatestPhysicalPartition().getId()).orElseThrow();
            MaterializedView.BasePartitionInfo recorded = mv.getRefreshScheme().getAsyncRefreshContext()
                    .getBaseTableVisibleVersionMap().get(fact.getId()).get(name);
            Assertions.assertEquals(partition.getId(), recorded.getId(), name);
            Assertions.assertEquals(captured.getVisibleVersion(), recorded.getVersion(), name);
            Assertions.assertEquals(captured.getVisibleVersionTimeMs(), recorded.getLastRefreshTime(), name);
        }
    }

    /**
     * The DML is faked, so the commit callback that promotes the staged deltas into the baseline never runs; do
     * its job. Returns base table id -> delta end.
     */
    protected static Map<Long, Long> promoteStagedDeltas(MaterializedView mv) {
        MaterializedView.AsyncRefreshContext context = mv.getRefreshScheme().getAsyncRefreshContext();
        Map<Long, Long> deltaEnds = new HashMap<>();
        for (Map.Entry<BaseTableInfo, TvrVersionRange> staged : context.getTempBaseTableInfoTvrDeltaMap().entrySet()) {
            long deltaEnd = staged.getValue().end().orElseThrow();
            context.getBaseTableInfoTvrVersionRangeMap().put(staged.getKey(), TvrTableSnapshot.of(deltaEnd));
            deltaEnds.put(staged.getKey().getTableId(), deltaEnd);
        }
        context.clearTempBaseTableInfoTvrDeltaState();
        return deltaEnds;
    }

    protected static void failIncrementalPlanning() {
        new MockUp<MVIVMRefreshProcessor>() {
            @Mock
            public MVRefreshProcessor.ProcessExecPlan getProcessExecPlan(TaskRunContext taskRunContext) {
                throw new SemanticException("incremental planning failed (simulated)");
            }
        };
    }

    protected static TaskRun runFirstBatch(MaterializedView mv) throws Exception {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(mv.getId()));
        task.getProperties().put(TaskRun.IS_TEST, "true");
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        executeBatch(taskRun);
        return taskRun;
    }

    protected static Constants.TaskRunState executeBatch(TaskRun taskRun) throws Exception {
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        return taskRun.executeTaskRun();
    }

    /** The batch {@code executed} spawned, or null when it was the job's last one. */
    protected static TaskRun nextOf(TaskRun executed) {
        MVRefreshProcessor processor = refreshProcessorOf(executed);
        if (processor instanceof MVHybridRefreshProcessor hybrid) {
            return hybrid.getCurrentProcessor().getNextTaskRun();
        }
        return processor.getNextTaskRun();
    }

    protected static MVRefreshProcessor refreshProcessorOf(TaskRun taskRun) {
        return ((MVTaskRunProcessor) taskRun.getProcessor()).getMVRefreshProcessor();
    }

    protected static long frozenBookmarkId(MaterializedView mv) {
        Map<BaseTableInfo, TvrVersionRange> frozen =
                mv.getRefreshScheme().getAsyncRefreshContext().getTempBaseTableInfoTvrDeltaMap();
        Assertions.assertEquals(1, frozen.size(), "frozen map: " + frozen);
        return frozen.values().iterator().next().end().orElseThrow();
    }

    protected static long frozenBookmarkIdOf(MaterializedView mv, OlapTable table) {
        return bookmarkIdOf(mv.getRefreshScheme().getAsyncRefreshContext().getTempBaseTableInfoTvrDeltaMap(), table);
    }

    protected static long bookmarkIdOf(Map<BaseTableInfo, TvrVersionRange> tvrMap, OlapTable table) {
        return tvrMap.entrySet().stream()
                .filter(entry -> entry.getKey().getTableId() == table.getId())
                .findFirst().orElseThrow(() -> new AssertionError(table.getName() + " is not in " + tvrMap))
                .getValue().end().orElseThrow();
    }

    protected static long partitionVersionIn(OlapTable fact, long bookmarkId, String name) {
        Partition partition = fact.getPartition(name);
        return GlobalStateMgr.getCurrentState().getBookmarkManager()
                .findBookmarkById(db.getId(), fact.getId(), bookmarkId).orElseThrow()
                .getPhysicalPartitionVersion(partition.getId(), partition.getDefaultPhysicalPartition().getId())
                .orElseThrow();
    }

    protected static long recordedVersion(MaterializedView mv, OlapTable fact, String name) {
        return mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap()
                .get(fact.getId()).get(name).getVersion();
    }

    protected static long livePartitionVersion(OlapTable fact, String name) {
        return fact.getPartition(name).getDefaultPhysicalPartition().getVisibleVersion();
    }

    protected static Map<String, Long> liveVersions(OlapTable fact) {
        Map<String, Long> versions = new TreeMap<>();
        for (String name : PARTITIONS) {
            versions.put(name, livePartitionVersion(fact, name));
        }
        return versions;
    }

    protected static void loadPartition(OlapTable fact, String name) {
        PhysicalPartition partition = fact.getPartition(name).getDefaultPhysicalPartition();
        partition.setVisibleVersion(partition.getVisibleVersion() + 1, System.currentTimeMillis());
        partition.setDataVersion(partition.getDataVersion() + 1);
    }

    protected static void loadEveryPartition(OlapTable fact) {
        for (String name : PARTITIONS) {
            loadPartition(fact, name);
        }
    }

    protected static OlapTable createFact(String name) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + name + " (dt DATE NOT NULL, k INT NOT NULL, v BIGINT) " +
                "DUPLICATE KEY(dt, k) PARTITION BY RANGE(dt) (" +
                "PARTITION p1 VALUES LESS THAN ('2024-02-01'), PARTITION p2 VALUES LESS THAN ('2024-03-01'), " +
                "PARTITION p3 VALUES LESS THAN ('2024-04-01'), PARTITION p4 VALUES LESS THAN ('2024-05-01')) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) db.getTable(name);
    }

    /** An unpartitioned table: its only partition is named after the table. */
    protected static OlapTable createDim(String name) throws Exception {
        starRocksAssert.withTable("CREATE TABLE " + name + " (k INT NOT NULL, w BIGINT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        return (OlapTable) db.getTable(name);
    }

    protected static MaterializedView createMv(String name, OlapTable fact, String refreshMode) throws Exception {
        return createMv(name, refreshMode, "SELECT dt, k, SUM(v) AS total FROM " + fact.getName() + " GROUP BY dt, k");
    }

    /** Partitioned by the fact table, so the dim table is a non-ref base table. */
    protected static MaterializedView createJoinMv(String name, OlapTable fact, OlapTable dim, String refreshMode)
            throws Exception {
        return createMv(name, refreshMode, "SELECT f.dt, f.k, SUM(f.v * d.w) AS total FROM " + fact.getName() +
                " f JOIN " + dim.getName() + " d ON f.k = d.k GROUP BY f.dt, f.k");
    }

    protected static MaterializedView createMv(String name, String refreshMode, String query) throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + name + " PARTITION BY dt " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('refresh_mode' = '" + refreshMode + "', 'partition_refresh_number' = '1', " +
                "'partition_refresh_strategy' = 'strict') AS " + query);
        MaterializedView mv = (MaterializedView) db.getTable(name);
        Assertions.assertTrue(mv.isCloudNativeMaterializedView());
        return mv;
    }

    protected static void mockLakeDml() {
        mockLakeDml(insertStmt -> {
        });
    }

    /**
     * The shared DML fake bumps replica versions through LocalTablet, which a cloud-native MV does not have.
     * This one only records what each scan read and bumps the refreshed MV partitions' visible version, after
     * handing the statement to {@code onWrite}.
     */
    protected static void mockLakeDml(Consumer<InsertStmt> onWrite) {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) {
                if (!(stmt instanceof InsertStmt insertStmt)) {
                    return;
                }
                onWrite.accept(insertStmt);
                Map<String, Map<Long, Long>> scanned = new HashMap<>();
                for (ScanNode node : execPlan.getScanNodes()) {
                    if (node instanceof OlapScanNode olapScanNode) {
                        scanned.put(node.getTableName(), new HashMap<>(olapScanNode.getScanPartitionVersions()));
                    }
                }
                SCANNED_VERSIONS.add(scanned);
                TableName tableName = TableName.fromTableRef(insertStmt.getTableRef());
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(tableName.getDb(), tableName.getTbl());
                if (!(table instanceof OlapTable olapTable)) {
                    return;
                }
                for (Long partitionId : insertStmt.getTargetPartitionIds()) {
                    PhysicalPartition partition = olapTable.getPartition(partitionId).getDefaultPhysicalPartition();
                    partition.setVisibleVersion(partition.getVisibleVersion() + 1, System.currentTimeMillis());
                }
            }
        };
    }
}
