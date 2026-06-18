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
import org.mockito.Mockito;

import java.util.List;

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
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
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
