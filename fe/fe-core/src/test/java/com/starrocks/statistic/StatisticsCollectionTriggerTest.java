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
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.InMemoryStatisticStorage;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class StatisticsCollectionTriggerTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        GlobalStateMgr.getCurrentState().setStatisticStorage(new InMemoryStatisticStorage());
    }

    @AfterClass
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
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true);
            Assert.assertEquals(null, trigger.getAnalyzeType());
        }
        {
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1000, 5);
            transactionState.setTxnCommitAttachment(attachment);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true);
            Assert.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
        }

        {
            InsertTxnCommitAttachment attachment = new InsertTxnCommitAttachment(1000000, 5);
            transactionState.setTxnCommitAttachment(attachment);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnFirstLoad(transactionState, db, table, true, true);
            Assert.assertEquals(StatsConstants.AnalyzeType.SAMPLE, trigger.getAnalyzeType());
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

            Assert.assertTrue(insertOverwriteSQLs.get(0).contains(String.format("SELECT    table_id, %d, column_name",
                    targetId)));
            Assert.assertTrue(insertOverwriteSQLs.get(0).contains(String.format("`partition_id`=%d", sourceId)));
            Assert.assertTrue(insertOverwriteSQLs.get(1).contains(String.format("DELETE FROM column_statistics\n" +
                    "WHERE `table_id`=%d AND `partition_id`=%d", table.getId(), sourceId)));

            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnInsertOverwrite(stats, db, table, true, true);
            Assert.assertNull(trigger.getAnalyzeType());
        }

        // case: overwrite a lot of data, need to re-collect statistics
        {
            InsertOverwriteJobStats stats = new InsertOverwriteJobStats(
                    List.of(sourceId), List.of(targetId), 1000, 50000);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnInsertOverwrite(stats, db, table, true, true);
            Assert.assertEquals(StatsConstants.AnalyzeType.FULL, trigger.getAnalyzeType());
        }

        // case: overwrite a lot of data, need to re-collect statistics
        {
            InsertOverwriteJobStats stats = new InsertOverwriteJobStats(
                    List.of(sourceId), List.of(targetId), 1000, 50000000);
            StatisticsCollectionTrigger trigger =
                    StatisticsCollectionTrigger.triggerOnInsertOverwrite(stats, db, table, true, true);
            Assert.assertEquals(StatsConstants.AnalyzeType.SAMPLE, trigger.getAnalyzeType());
        }
    }
}