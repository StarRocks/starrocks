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

package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
<<<<<<< HEAD
=======
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
>>>>>>> 389d5f42d32... [BugFix][RB-712] Protect first-load statistics row counts with table locks (#66819)
import com.starrocks.qe.DmlType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.InMemoryStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.List;
<<<<<<< HEAD
=======
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
>>>>>>> 389d5f42d32... [BugFix][RB-712] Protect first-load statistics row counts with table locks (#66819)

public class StatisticsCollectionTriggerTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        GlobalStateMgr.getCurrentState().setStatisticStorage(new InMemoryStatisticStorage());
    }

    @AfterAll
    public static void afterClass() {
        PlanTestBase.afterClass();
    }

    @Test
    public void triggerOnLoad() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_load";
        starRocksAssert.withDatabase(dbName)
                .useDatabase(dbName);
        starRocksAssert.withTable("create table t_load (" +
                "c1 int not null," +
                "c2 int not null" +
                ") " +
                "partition by (c1)\n" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t_load add partition p1 values in ('1')");
        starRocksAssert.ddl("alter table t_load add partition p2 values in ('2')");
        starRocksAssert.ddl("alter table t_load add partition p3 values in ('3')");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = db.getTable(tableName);
        Partition partition = table.getPartition("p1");

        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(partition.getDefaultPhysicalPartition().getId(), 2, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);

        setPartitionStatistics((OlapTable) table, "p1", 1000);
        {
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1, 5);
            transactionState.setTxnCommitAttachment(attachment);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                            DmlType.INSERT_INTO);
            Assertions.assertEquals(null, trigger.getAnalyzeType());
        }
        {
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1000, 5);
            transactionState.setTxnCommitAttachment(attachment);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                            DmlType.INSERT_INTO);
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
        }

        {
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1000000, 5);
            transactionState.setTxnCommitAttachment(attachment);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                            DmlType.INSERT_INTO);
            Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, trigger.getAnalyzeType());
        }
    }

    @Test
<<<<<<< HEAD
=======
    public void triggerOnFirstLoadUsesCandidatePartitionSize() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_first_load_large_history";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");
        starRocksAssert.ddl("alter table " + tableName + " add partition p_history values in ('2')");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = (OlapTable) db.getTable(tableName);
        setPartitionStatistics(table, "p_history", 1_000_000_000L);
        BasicStatsMeta statsMeta = new BasicStatsMeta(db.getId(), table.getId(), List.of(),
                StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Map.of(), 1_000_000_000L);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().put(table.getId(), statsMeta);

        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                table.getPartition("p1").getDefaultPhysicalPartition().getId(), Partition.PARTITION_INIT_VERSION + 1, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);

        // A new partition's FE row count may still be zero, or may have already been synchronized.
        for (long partitionRows : new long[] {0, 1000}) {
            setPartitionStatistics(table, "p1", partitionRows);
            for (long loadedRows : new long[] {1000, Config.statistic_sample_collect_rows,
                    Config.statistic_sample_collect_rows + 1}) {
                transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(loadedRows));
                StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                        transactionState, db, table, true, true, DmlType.INSERT_INTO);
                StatsConstants.AnalyzeType expectedType = loadedRows > Config.statistic_sample_collect_rows
                        ? StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL;
                Assertions.assertEquals(expectedType, trigger.getAnalyzeType(),
                        "first load with " + loadedRows + " rows and FE partition row count " + partitionRows);
            }

            transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(1));
            StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                    transactionState, db, table, true, true, DmlType.INSERT_INTO);
            if (partitionRows == 0) {
                Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
            } else {
                Assertions.assertNull(trigger.getAnalyzeType());
            }
        }
    }

    @Test
    public void triggerOnFirstLoadSumsOnlyCandidatePartitions() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_first_load_multiple_partitions";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");
        starRocksAssert.ddl("alter table " + tableName + " add partition p2 values in ('2')");
        starRocksAssert.ddl("alter table " + tableName + " add partition p_existing values in ('3')");
        starRocksAssert.ddl("alter table " + tableName + " add partition p_history values in ('4')");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = (OlapTable) db.getTable(tableName);
        setPartitionStatistics(table, "p1", 1000);
        setPartitionStatistics(table, "p2", 9000);
        setPartitionStatistics(table, "p_existing", 1_000_000_000L);
        setPartitionStatistics(table, "p_history", 1_000_000_000L);
        BasicStatsMeta statsMeta = new BasicStatsMeta(db.getId(), table.getId(), List.of(),
                StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Map.of(), 2_000_010_000L);
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().put(table.getId(), statsMeta);

        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        for (String partitionName : List.of("p1", "p2")) {
            commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                    table.getPartition(partitionName).getDefaultPhysicalPartition().getId(),
                    Partition.PARTITION_INIT_VERSION + 1, 1));
        }
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                table.getPartition("p_existing").getDefaultPhysicalPartition().getId(),
                Partition.PARTITION_INIT_VERSION + 5, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);

        // Both first-load partitions count; neither the touched existing partition nor untouched history counts.
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(500));
        StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertNull(trigger.getAnalyzeType());

        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(2000));
        trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());

        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(Config.statistic_sample_collect_rows + 1));
        trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, trigger.getAnalyzeType());
    }

    @Test
    public void triggerOnFirstPhysicalLoadUsesLogicalPartitionSize() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_first_load_new_physical";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = (OlapTable) db.getTable(tableName);
        Partition partition = table.getPartition("p1");
        PhysicalPartition existingPhysical = partition.getDefaultPhysicalPartition();
        existingPhysical.updateVisibleVersion(Partition.PARTITION_INIT_VERSION + 5);
        existingPhysical.setDataVersion(Partition.PARTITION_INIT_VERSION + 5);
        setPartitionStatistics(table, "p1", 9000);

        MaterializedIndex newIndex = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId());
        newIndex.setRowCount(1000);
        PhysicalPartition newPhysical = new PhysicalPartition(
                GlobalStateMgr.getCurrentState().getNextId(), partition.getId(), newIndex);
        newPhysical.updateVisibleVersion(Partition.PARTITION_INIT_VERSION + 1);
        newPhysical.setDataVersion(Partition.PARTITION_INIT_VERSION + 1);
        partition.addSubPartition(newPhysical);
        table.addPhysicalPartition(newPhysical);

        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                newPhysical.getId(), Partition.PARTITION_INIT_VERSION + 1, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);

        // The candidate is the logical partition, including rows in its previously loaded physical partition.
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(500));
        StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertNull(trigger.getAnalyzeType());

        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(2000));
        trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
    }

    @Test
    public void triggerOnFirstLoadSkipsEmptyOrUnknownLoad() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_first_load_empty";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = db.getTable(tableName);
        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                table.getPartition("p1").getDefaultPhysicalPartition().getId(), Partition.PARTITION_INIT_VERSION + 1, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);

        for (long loadedRows : new long[] {0, -1}) {
            transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(loadedRows));
            StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                    transactionState, db, table, true, true, DmlType.INSERT_INTO);
            Assertions.assertNull(trigger.getAnalyzeType());
        }

        transactionState.setTxnCommitAttachment(null);
        StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertNull(trigger.getAnalyzeType());
    }

    @ParameterizedTest
    @EnumSource(value = DmlType.class, names = {"INSERT_INTO", "UPDATE"})
    public void triggerOnLoadProtectsRowCountsButDoesNotLockCollection(DmlType dmlType) throws Exception {
        final String dbName = "test_statistics";
        final String tableName = dmlType == DmlType.INSERT_INTO ? "t_insert_row_count_lock" : "t_update_row_count_lock";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = Mockito.spy((OlapTable) db.getTable(tableName));
        Partition partition = Mockito.spy(table.getPartition("p1"));
        long partitionId = partition.getId();
        Mockito.doReturn(Optional.of(partition)).when(table).mayGetPartition(partitionId);
        if (dmlType == DmlType.INSERT_INTO) {
            // INSERT must protect the partition read even when table statistics are already available.
            BasicStatsMeta statsMeta = new BasicStatsMeta(db.getId(), table.getId(), List.of(),
                    StatsConstants.AnalyzeType.SAMPLE, LocalDateTime.now(), Map.of(), 1_000_000_000L);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().put(table.getId(), statsMeta);
        } else {
            // UPDATE falls back to the same partition read when no table statistics exist.
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().remove(table.getId());
        }

        ExecutorService writer = Executors.newSingleThreadExecutor();
        try {
            Mockito.doAnswer(invocation -> {
                // Wait for an actual competing writer while getRowCount is still on the reader's stack.
                // A bounded lock attempt makes this deterministic without scheduling sleeps.
                Assertions.assertFalse(writer.submit(() -> tryTableWriteLock(db, table))
                        .get(5, TimeUnit.SECONDS), "a writer must not enter while partition row counts are read");
                return 1000L;
            }).when(partition).getRowCount();

            AtomicBoolean collectionCanWrite = new AtomicBoolean();
            new MockUp<StatisticExecutor>() {
                @Mock
                public AnalyzeStatus collectStatistics(ConnectContext context, StatisticsCollectJob job,
                                                        AnalyzeStatus status, boolean refreshAsync,
                                                        boolean resetWarehouse) {
                    // This runs on the real analyze worker while the triggering thread waits for it.
                    collectionCanWrite.set(tryTableWriteLock(db, table));
                    return status;
                }
            };

            TransactionState transactionState = firstLoadTransaction(table, partition);
            StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                    transactionState, db, table, true, true, dmlType);

            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
            Mockito.verify(partition).getRowCount();
            Assertions.assertTrue(collectionCanWrite.get(),
                    "the table lock must be released before collection runs and the trigger waits for completion");
        } finally {
            writer.shutdownNow();
            Assertions.assertTrue(writer.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void triggerOnLoadReleasesTableLockWhenReadingRowsFails() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_load_row_count_failure";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = Mockito.spy((OlapTable) db.getTable(tableName));
        Partition partition = Mockito.spy(table.getPartition("p1"));
        long partitionId = partition.getId();
        Mockito.doReturn(Optional.of(partition)).when(table).mayGetPartition(partitionId);
        IllegalStateException failure = new IllegalStateException("failed to read partition rows");
        Mockito.doThrow(failure).when(partition).getRowCount();

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class, () ->
                StatisticsCollectionTrigger.triggerOnFirstLoad(
                        firstLoadTransaction(table, partition), db, table, true, true, DmlType.INSERT_INTO)));

        ExecutorService writer = Executors.newSingleThreadExecutor();
        try {
            Assertions.assertTrue(writer.submit(() -> tryTableWriteLock(db, table)).get(5, TimeUnit.SECONDS),
                    "a failed partition read must release the table lock");
        } finally {
            writer.shutdownNow();
            Assertions.assertTrue(writer.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private TransactionState firstLoadTransaction(OlapTable table, Partition partition) {
        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                partition.getDefaultPhysicalPartition().getId(), Partition.PARTITION_INIT_VERSION + 1, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(1000));
        return transactionState;
    }

    private boolean tryTableWriteLock(Database db, Table table) {
        Locker locker = new Locker();
        boolean acquired = locker.tryLockTableWithIntensiveDbLock(
                db.getId(), table.getId(), LockType.WRITE, 100, TimeUnit.MILLISECONDS);
        if (acquired) {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        }
        return acquired;
    }

    @Test
    public void triggerOnInsertSkipsPreviouslyLoadedPartition() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_repeated_load";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        createPartitionedTable(tableName, "");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = db.getTable(tableName);
        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                table.getPartition("p1").getDefaultPhysicalPartition().getId(), Partition.PARTITION_INIT_VERSION + 5, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(Config.statistic_sample_collect_rows + 1));

        StatisticsCollectionTrigger trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(
                transactionState, db, table, true, true, DmlType.INSERT_INTO);
        Assertions.assertNull(trigger.getAnalyzeType());
    }

    @Test
>>>>>>> 389d5f42d32... [BugFix][RB-712] Protect first-load statistics row counts with table locks (#66819)
    public void triggerOnLoadSkipsPartitionRemovedByConcurrentOptimize() throws Exception {
        // Regression: during online optimize, a load txn becomes VISIBLE and its post-commit, best-effort
        // first-load stats trigger iterates the txn's PartitionCommitInfo. A concurrent OnlineOptimizeJobV2 can
        // replacePartition + disableDoubleWritePartition in between, so the committed partition id no longer
        // resolves on the table. Previously prepareAnalyzeJobForLoad dereferenced it unguarded and NPE'd,
        // surfacing an already-committed load to the client as FAILED. The trigger must skip the missing
        // partition (and still process the surviving ones) instead.
        final String dbName = "test_statistics";
        final String tableName = "t_load_optimize_race";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("create table " + tableName + " (" +
                "c1 int not null," +
                "c2 int not null" +
                ") " +
                "partition by (c1)\n" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table " + tableName + " add partition p1 values in ('1')");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = db.getTable(tableName);

        // Case 1 (guard #1, physicalPartition == null): a commit info whose physical partition was swapped out
        // by a concurrent optimize -> use an id that is not assigned to any partition of the table.
        // Must not throw (pre-fix this NPE'd at getParentId()); with nothing resolvable there is nothing to collect.
        {
            long vanishedPhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
            TransactionState transactionState = new TransactionState();
            TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
            commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(vanishedPhysicalPartitionId, 2, 1));
            transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);
            transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(1000, 5));

            StatisticsCollectionTrigger trigger = Assertions.assertDoesNotThrow(() ->
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                            DmlType.INSERT_INTO));
            Assertions.assertNull(trigger.getAnalyzeType());
        }

        // Case 2 (pin `continue` semantics): a vanished partition alongside a still-valid one. The vanished entry
        // must be skipped while the valid partition is still collected -> proves we `continue` rather than abort
        // the whole loop on the first miss.
        {
            Partition valid = table.getPartition("p1");
            setPartitionStatistics((OlapTable) table, "p1", 1000);
            TransactionState transactionState = new TransactionState();
            TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
            commitInfo.addPartitionCommitInfo(
                    new PartitionCommitInfo(GlobalStateMgr.getCurrentState().getNextId(), 2, 1)); // vanished
            commitInfo.addPartitionCommitInfo(
                    new PartitionCommitInfo(valid.getDefaultPhysicalPartition().getId(), 2, 1));  // survives
            transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);
            transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(1000, 5));

            StatisticsCollectionTrigger trigger = Assertions.assertDoesNotThrow(() ->
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                            DmlType.INSERT_INTO));
            // Valid partition was still picked up -> collection proceeds (FULL for a 1000-row first load).
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
        }

        // Case 3 (guard #2, physicalPartition != null but parent logical partition == null): the physical
        // partition still resolves but its parent logical partition was removed by the concurrent optimize.
        // Use a spy so getPhysicalPartition returns a stub whose parentId points at a non-existent logical id.
        {
            OlapTable spyTable = Mockito.spy((OlapTable) table);
            long orphanPhysicalId = GlobalStateMgr.getCurrentState().getNextId();
            long missingParentLogicalId = GlobalStateMgr.getCurrentState().getNextId();
            PhysicalPartition orphan = Mockito.mock(PhysicalPartition.class);
            Mockito.when(orphan.getParentId()).thenReturn(missingParentLogicalId);
            Mockito.doReturn(orphan).when(spyTable).getPhysicalPartition(orphanPhysicalId);
            // real getPartition(missingParentLogicalId) returns null since that id is unassigned -> hits guard #2.

            TransactionState transactionState = new TransactionState();
            TableCommitInfo commitInfo = new TableCommitInfo(spyTable.getId());
            commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(orphanPhysicalId, 2, 1));
            transactionState.putIdToTableCommitInfo(spyTable.getId(), commitInfo);
            transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(1000, 5));

            StatisticsCollectionTrigger trigger = Assertions.assertDoesNotThrow(() ->
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, spyTable, true, true,
                            DmlType.INSERT_INTO));
            Assertions.assertNull(trigger.getAnalyzeType());
        }
    }

    @Test
    public void triggerOnInsertOverwrite() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_overwrite";
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        starRocksAssert.withDatabase(dbName)
                .useDatabase(dbName);
        starRocksAssert.withTable("create table t_overwrite (" +
                "c1 int not null," +
                "c2 int not null" +
                ") " +
                "partition by (c1)\n" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t_overwrite add partition p1 values in ('1')");
        starRocksAssert.ddl("alter table t_overwrite add partition p2 values in ('2')");
        starRocksAssert.ddl("alter table t_overwrite add partition p3 values in ('3')");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = db.getTable(tableName);
        long sourceId = table.getPartition("p1").getId();
        long targetId = GlobalStateMgr.getCurrentState().getNextId();
        storage.updatePartitionStatistics(table.getId(), sourceId, 1000);

        // case: overwrite almost same number of rows
        {
            InsertOverwriteJobStats stats = new InsertOverwriteJobStats(
                    List.of(sourceId), List.of(targetId), 1000, 1001);

            List<String> insertOverwriteSQLs = FullStatisticsCollectJob.buildOverwritePartitionSQL(
                    table.getId(), sourceId, targetId);

            Assertions.assertTrue(insertOverwriteSQLs.get(0).contains(String.format("SELECT    table_id, %d, column_name",
                    targetId)));
            Assertions.assertTrue(insertOverwriteSQLs.get(0).contains(String.format("`partition_id`=%d", sourceId)));

            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnInsertOverwrite(stats, db, table, true, true);
            Assertions.assertNull(trigger.getAnalyzeType());
        }

        // case: overwrite a lot of data, need to re-collect statistics
        {
            InsertOverwriteJobStats stats = new InsertOverwriteJobStats(
                    List.of(sourceId), List.of(targetId), 1000, 50000);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnInsertOverwrite(stats, db, table, true, true);
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
        }

        // case: overwrite a lot of data, need to re-collect statistics
        {
            InsertOverwriteJobStats stats = new InsertOverwriteJobStats(
                    List.of(sourceId), List.of(targetId), 1000, 50000000);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnInsertOverwrite(stats, db, table, true, true);
            Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, trigger.getAnalyzeType());
        }
    }

    @Test
    public void triggerOnUpdate() throws Exception {
        final String dbName = "test_statistics";
        final String tableName = "t_update";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("create table t_update (" +
                "c1 int not null," +
                "c2 int not null" +
                ") " +
                "partition by (c1)\n" +
                "properties('replication_num'='1')");
        starRocksAssert.ddl("alter table t_update add partition p1 values in ('1')");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        Table table = db.getTable(tableName);
        Partition partition = table.getPartition("p1");
        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(
                partition.getDefaultPhysicalPartition().getId(), Partition.PARTITION_INIT_VERSION + 5, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);

        setPartitionStatistics((OlapTable) table, "p1", 1000);

        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(5));
        StatisticsCollectionTrigger trigger =
                StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                        DmlType.UPDATE);
        Assertions.assertNull(trigger.getAnalyzeType());

        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(500));
        trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                DmlType.UPDATE);
        Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());

        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(500000));
        trigger = StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                DmlType.UPDATE);
        Assertions.assertEquals(StatsConstants.AnalyzeType.SAMPLE, trigger.getAnalyzeType());
    }

    @Test
    public void testTableLevelFirstLoadConfigOverridesGlobalWhenExplicitlySet() throws Exception {
        boolean oldEnableStatisticCollectOnFirstLoad = Config.enable_statistic_collect_on_first_load;
        try {
            final String dbName = "test_statistics_first_load_override";
            starRocksAssert.withDatabase(dbName).useDatabase(dbName);
            createPartitionedTable("t_follow_global", "");
            createPartitionedTable("t_disable_table",
                    ", 'enable_statistic_collect_on_first_load' = 'false'");
            createPartitionedTable("t_enable_table",
                    ", 'enable_statistic_collect_on_first_load' = 'true'");

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);

            Config.enable_statistic_collect_on_first_load = false;
            StatisticsCollectionTrigger trigger = triggerFirstLoad(db, "t_follow_global");
            Assertions.assertNull(trigger.getAnalyzeType());

            Config.enable_statistic_collect_on_first_load = true;
            trigger = triggerFirstLoad(db, "t_disable_table");
            Assertions.assertNull(trigger.getAnalyzeType());

            Config.enable_statistic_collect_on_first_load = false;
            trigger = triggerFirstLoad(db, "t_enable_table");
            Assertions.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
        } finally {
            Config.enable_statistic_collect_on_first_load = oldEnableStatisticCollectOnFirstLoad;
        }
    }

    private void createPartitionedTable(String tableName, String extraProperties) throws Exception {
        starRocksAssert.withTable("create table " + tableName + " (" +
                "c1 int not null," +
                "c2 int not null" +
                ") " +
                "partition by (c1)\n" +
                "properties('replication_num'='1'" + extraProperties + ")");
        starRocksAssert.ddl("alter table " + tableName + " add partition p1 values in ('1')");
    }

    private StatisticsCollectionTrigger triggerFirstLoad(Database db, String tableName) {
        Table table = db.getTable(tableName);
        Partition partition = table.getPartition("p1");
        TransactionState transactionState = new TransactionState();
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(partition.getDefaultPhysicalPartition().getId(), 2, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);
        transactionState.setTxnCommitAttachment(new InsertTxnCommitAttachment(1000, 5));
        setPartitionStatistics((OlapTable) table, "p1", 1000);
        return StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true,
                DmlType.INSERT_INTO);
    }
}
