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

package com.starrocks.analysis;

import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvRewriteTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDate;
import java.time.Period;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RefreshMaterializedViewTest  extends MvRewriteTestBase {
    // 1hour: set it to 1 hour to avoid FE's async update too late.
    private static final long MV_STALENESS = 60 * 60;

    @BeforeClass
    public static void beforeClass() throws Exception {
        MvRewriteTestBase.beforeClass();
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE test.tbl_with_mv\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view mv_to_refresh\n" +
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh manual\n" +
                        "as select k2, sum(v1) as total from tbl_with_mv group by k2;")
                .withMaterializedView("create materialized view mv2_to_refresh\n" +
                        "PARTITION BY k1\n"+
                        "distributed by hash(k2) buckets 3\n" +
                        "refresh manual\n" +
                        "as select k1, k2, v1  from tbl_with_mv;");
    }

    @Test
    public void testNormal() throws Exception {
        String refreshMvSql = "refresh materialized view test.mv_to_refresh";
        RefreshMaterializedViewStatement alterMvStmt =
                (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(refreshMvSql, connectContext);
        String dbName = alterMvStmt.getMvName().getDb();
        String mvName = alterMvStmt.getMvName().getTbl();
        Assert.assertEquals("test", dbName);
        Assert.assertEquals("mv_to_refresh", mvName);

        String sql = "REFRESH MATERIALIZED VIEW test.mv2_to_refresh PARTITION START('2022-02-03') END ('2022-02-25') FORCE;";
        RefreshMaterializedViewStatement statement = (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assert.assertTrue(statement.isForceRefresh());
        Assert.assertEquals("2022-02-03", statement.getPartitionRangeDesc().getPartitionStart());
        Assert.assertEquals("2022-02-25", statement.getPartitionRangeDesc().getPartitionEnd());

        try {
            sql = "REFRESH MATERIALIZED VIEW test.mv_to_refresh PARTITION START('2022-02-03') END ('2022-02-25') FORCE;";
            statement = (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 1, column 26 to line 1, column 31. " +
                    "Detail message: Not support refresh by partition for single partition mv.", e.getMessage());
        }

        try {
            sql = "REFRESH MATERIALIZED VIEW test.mv2_to_refresh PARTITION START('2022-02-03') END ('2020-02-25') FORCE;";
            statement = (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 1, column 56 to line 1, column 93. " +
                    "Detail message: Batch build partition start date should less than end date.", e.getMessage());
        }

        try {
            sql = "REFRESH MATERIALIZED VIEW test.mv2_to_refresh PARTITION START('dhdfghg') END ('2020-02-25') FORCE;";
            statement = (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error from line 1, column 56 to line 1, column 90. " +
                    "Detail message: Batch build partition EVERY is date type but START or END does not type match.",
                    e.getMessage());
        }
    }

    @Test
    public void testRefreshExecution() throws Exception {
        executeInsertSql(connectContext, "insert into tbl_with_mv values(\"2022-02-20\", 1, 10)");
        refreshMaterializedView("test", "mv_to_refresh");
        MaterializedView mv1 = getMv("test", "mv_to_refresh");
        Set<String> partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv1);
        Assert.assertTrue(partitionsToRefresh1.isEmpty());
        refreshMaterializedView("test", "mv2_to_refresh");
        MaterializedView mv2 = getMv("test", "mv2_to_refresh");
        Set<String> partitionsToRefresh2 = getPartitionNamesToRefreshForMv(mv2);
        Assert.assertTrue(partitionsToRefresh2.isEmpty());

        executeInsertSql(connectContext, "insert into tbl_with_mv partition(p2) values(\"2022-02-20\", 2, 10)");
        OlapTable table = (OlapTable) getTable("test", "tbl_with_mv");
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        if (p2.getVisibleVersion() == 3) {
            partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv1);
            Assert.assertEquals(Sets.newHashSet("mv_to_refresh"), partitionsToRefresh1);
            partitionsToRefresh2 = getPartitionNamesToRefreshForMv(mv2);
            Assert.assertTrue(partitionsToRefresh2.contains("p2"));
        } else {
            // publish version is async, so version update may be late
            // for debug
            System.out.println("p1 visible version:" + p1.getVisibleVersion());
            System.out.println("p2 visible version:" + p2.getVisibleVersion());
            System.out.println("mv1 refresh context" + mv1.getRefreshScheme().getAsyncRefreshContext());
            System.out.println("mv2 refresh context" + mv2.getRefreshScheme().getAsyncRefreshContext());
        }
    }

    @Test
    public void testMaxMVRewriteStaleness1() {

        starRocksAssert.withTable(new MTable("tbl_staleness1", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p1 values [('2022-02-01'),('2022-02-16'))",
                                "PARTITION p2 values [('2022-02-16'),('2022-03-01'))"
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness\n" +
                            "PARTITION BY k1\n"+
                            "distributed by hash(k2) buckets 3\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\"" +
                            ")" +
                            "refresh manual\n" +
                            "as select k1, k2, v1  from tbl_staleness1;");

                    // refresh partitions are not empty if base table is updated.
                    executeInsertSql(connectContext, "insert into tbl_staleness1 partition(p2) " +
                            "values(\"2022-02-20\", 1, 10)");
                    {
                        refreshMaterializedView("test", "mv_with_mv_rewrite_staleness");
                    }

                    // alter mv_rewrite_staleness
                    {
                        String alterMvSql = String.format("alter materialized view mv_with_mv_rewrite_staleness " +
                                "set (\"mv_rewrite_staleness_second\" = \"%s\")", MV_STALENESS);
                        AlterMaterializedViewStmt stmt =
                                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
                        GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(stmt);
                    }
                    // no refresh partitions if mv_rewrite_staleness is set.
                    executeInsertSql(connectContext, "insert into tbl_staleness1 values(\"2022-02-20\", 1, 10)");
                    {
                        MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
                        checkToRefreshPartitionsEmpty(mv1);
                    }

                    // no refresh partitions if there is no new data.
                    {
                        refreshMaterializedView("test", "mv_with_mv_rewrite_staleness");
                        MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness");
                        checkToRefreshPartitionsEmpty(mv2);
                    }
                    // no refresh partitions if there is new data & no refresh but is set `mv_rewrite_staleness`.
                    {
                        executeInsertSql(connectContext, "insert into tbl_staleness1 values(\"2022-02-22\", 1, 10)");
                        MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
                        checkToRefreshPartitionsEmpty(mv1);
                    }
                    starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness");
                }
        );
    }

    private void checkToRefreshPartitionsEmpty(MaterializedView mv) {
        Set<String> partitionsToRefresh = Sets.newHashSet();
        Assert.assertTrue(mv.getPartitionNamesToRefreshForMv(partitionsToRefresh, true));
        Assert.assertTrue(partitionsToRefresh.isEmpty());
    }

    @Test
    public void testMaxMVRewriteStaleness2() {
        starRocksAssert.withTable(new MTable("tbl_staleness2", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p1 values [('2022-02-01'),('2022-02-16'))",
                                "PARTITION p2 values [('2022-02-16'),('2022-03-01'))"
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness2 \n" +
                            "PARTITION BY k1\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\"," +
                            "\"mv_rewrite_staleness_second\" = \"" + MV_STALENESS + "\"" +
                            ")" +
                            "refresh manual\n" +
                            "as select k1, k2, v1  from tbl_staleness2;");

                    // refresh partitions are not empty if base table is updated.
                    {
                        executeInsertSql(connectContext, "insert into tbl_staleness2 partition(p2) values(\"2022-02-20\", 1, 10)");
                        refreshMaterializedView("test", "mv_with_mv_rewrite_staleness2");

                        Table tbl1 = getTable("test", "tbl_staleness2");
                        Optional<Long> maxPartitionRefreshTimestamp =
                                tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
                        Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

                        MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness2");
                        Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());
                        Assert.assertEquals(mv1.maxBaseTableRefreshTimestamp().get(), maxPartitionRefreshTimestamp.get());

                        long mvRefreshTimeStamp = mv1.getLastRefreshTime();
                        Assert.assertTrue(mvRefreshTimeStamp == maxPartitionRefreshTimestamp.get());
                        Assert.assertTrue(mv1.isStalenessSatisfied());
                    }

                    // no refresh partitions if mv_rewrite_staleness is set.
                    {
                        executeInsertSql(connectContext, "insert into tbl_staleness2 values(\"2022-02-20\", 2, 10)");

                        Table tbl1 = getTable("test", "tbl_staleness2");
                        Optional<Long> maxPartitionRefreshTimestamp =
                                tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
                        Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

                        MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness2");
                        Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());

                        long mvMaxBaseTableRefreshTimestamp = mv1.maxBaseTableRefreshTimestamp().get();
                        long tblMaxPartitionRefreshTimestamp = maxPartitionRefreshTimestamp.get();
                        Assert.assertEquals(mvMaxBaseTableRefreshTimestamp, tblMaxPartitionRefreshTimestamp);

                        long mvRefreshTimeStamp = mv1.getLastRefreshTime();
                        Assert.assertTrue(mvRefreshTimeStamp < tblMaxPartitionRefreshTimestamp);
                        Assert.assertTrue((tblMaxPartitionRefreshTimestamp - mvRefreshTimeStamp) / 1000 < MV_STALENESS);
                        Assert.assertTrue(mv1.isStalenessSatisfied());

                        Set<String> partitionsToRefresh = getPartitionNamesToRefreshForMv(mv1);
                        Assert.assertTrue(partitionsToRefresh.isEmpty());
                    }
                    starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness2");
                }
        );
    }

    @Test
    public void testMaxMVRewriteStaleness3() {
        starRocksAssert.withTable(new MTable("tbl_staleness3", "k2",
                        List.of(
                                "k1 date",
                                "k2 int",
                                "v1 int"
                        ),
                        "k1",
                        List.of(
                                "PARTITION p1 values [('2022-02-01'),('2022-02-16'))",
                                "PARTITION p2 values [('2022-02-16'),('2022-03-01'))"
                        )
                ),
                () -> {
                    starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness21 \n" +
                            "PARTITION BY k1\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\"," +
                            "\"mv_rewrite_staleness_second\" = \"" + MV_STALENESS + "\"" +
                            ")" +
                            "refresh manual\n" +
                            "as select k1, k2, v1  from tbl_staleness3;");
                    starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness22 \n" +
                            "PARTITION BY k1\n" +
                            "distributed by hash(k2) buckets 3\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\"," +
                            "\"mv_rewrite_staleness_second\" = \"" + MV_STALENESS + "\"" +
                            ")" +
                            "refresh manual\n" +
                            "as select k1, k2, count(1)  from mv_with_mv_rewrite_staleness21 group by k1, k2;");

                    {
                        executeInsertSql(connectContext, "insert into tbl_staleness3 partition(p2) values(\"2022-02-20\", 1, 10)");
                        refreshMaterializedView("test", "mv_with_mv_rewrite_staleness21");
                        refreshMaterializedView("test", "mv_with_mv_rewrite_staleness22");
                        {
                            Table tbl1 = getTable("test", "tbl_staleness3");
                            Optional<Long> maxPartitionRefreshTimestamp =
                                    tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
                            Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

                            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness21");
                            Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());
                            Assert.assertEquals(mv1.maxBaseTableRefreshTimestamp().get(), maxPartitionRefreshTimestamp.get());

                            long mvRefreshTimeStamp = mv1.getLastRefreshTime();
                            Assert.assertTrue(mvRefreshTimeStamp == maxPartitionRefreshTimestamp.get());
                            Assert.assertTrue(mv1.isStalenessSatisfied());
                        }

                        {
                            Table tbl1 = getTable("test", "mv_with_mv_rewrite_staleness21");
                            Optional<Long> maxPartitionRefreshTimestamp =
                                    tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
                            Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

                            MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness22");
                            Assert.assertTrue(mv2.maxBaseTableRefreshTimestamp().isPresent());
                            Assert.assertEquals(mv2.maxBaseTableRefreshTimestamp().get(), maxPartitionRefreshTimestamp.get());

                            long mvRefreshTimeStamp = mv2.getLastRefreshTime();
                            Assert.assertTrue(mvRefreshTimeStamp == maxPartitionRefreshTimestamp.get());
                            Assert.assertTrue(mv2.isStalenessSatisfied());
                        }
                    }

                    {
                        executeInsertSql(connectContext, "insert into tbl_staleness3 values(\"2022-02-20\", 2, 10)");
                        {
                            Table tbl1 = getTable("test", "tbl_staleness3");
                            Optional<Long> maxPartitionRefreshTimestamp =
                                    tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
                            Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

                            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness21");
                            Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());

                            long mvMaxBaseTableRefreshTimestamp = mv1.maxBaseTableRefreshTimestamp().get();
                            long tblMaxPartitionRefreshTimestamp = maxPartitionRefreshTimestamp.get();
                            Assert.assertEquals(mvMaxBaseTableRefreshTimestamp, tblMaxPartitionRefreshTimestamp);

                            long mvRefreshTimeStamp = mv1.getLastRefreshTime();
                            Assert.assertTrue(mvRefreshTimeStamp < tblMaxPartitionRefreshTimestamp);
                            Assert.assertTrue((tblMaxPartitionRefreshTimestamp - mvRefreshTimeStamp) / 1000 < MV_STALENESS);
                            Assert.assertTrue(mv1.isStalenessSatisfied());

                            Set<String> partitionsToRefresh = getPartitionNamesToRefreshForMv(mv1);
                            Assert.assertTrue(partitionsToRefresh.isEmpty());
                        }
                        {
                            Table tbl1 = getTable("test", "mv_with_mv_rewrite_staleness21");
                            Optional<Long> maxPartitionRefreshTimestamp =
                                    tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
                            Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

                            MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness22");
                            Assert.assertTrue(mv2.maxBaseTableRefreshTimestamp().isPresent());

                            long mvMaxBaseTableRefreshTimestamp = mv2.maxBaseTableRefreshTimestamp().get();
                            long tblMaxPartitionRefreshTimestamp = maxPartitionRefreshTimestamp.get();
                            Assert.assertEquals(mvMaxBaseTableRefreshTimestamp, tblMaxPartitionRefreshTimestamp);

                            long mvRefreshTimeStamp = mv2.getLastRefreshTime();
                            Assert.assertTrue(mvRefreshTimeStamp <= tblMaxPartitionRefreshTimestamp);
                            Assert.assertTrue((tblMaxPartitionRefreshTimestamp - mvRefreshTimeStamp) / 1000 < MV_STALENESS);
                            Assert.assertTrue(mv2.isStalenessSatisfied());

                            Set<String> partitionsToRefresh = getPartitionNamesToRefreshForMv(mv2);
                            Assert.assertTrue(partitionsToRefresh.isEmpty());
                        }
                    }

                    {
                        executeInsertSql(connectContext, "insert into tbl_staleness3 values(\"2022-02-20\", 2, 10)");
                        {
                            // alter mv_rewrite_staleness
                            {
                                String alterMvSql = "alter materialized view mv_with_mv_rewrite_staleness21 " +
                                        "set (\"mv_rewrite_staleness_second\" = \"0\")";
                                AlterMaterializedViewStmt stmt =
                                        (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
                                GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(stmt);
                            }

                            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness21");
                            Assert.assertFalse(mv1.isStalenessSatisfied());
                            Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());

                            MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness22");
                            Assert.assertFalse(mv1.isStalenessSatisfied());
                            Assert.assertFalse(mv2.maxBaseTableRefreshTimestamp().isPresent());

                            Set<String> partitionsToRefresh = getPartitionNamesToRefreshForMv(mv2);
                            Assert.assertFalse(partitionsToRefresh.isEmpty());
                        }
                    }
                    starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness21");
                    starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness22");
                }
        );
    }

    @Test
    public void testRefreshHourPartitionMv() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb(stmt.getTableName().getDb());
                    OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                    for (Partition partition : tbl.getPartitions()) {
                        if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                            long version = partition.getVisibleVersion() + 1;
                            partition.setVisibleVersion(version, System.currentTimeMillis());
                            MaterializedIndex baseIndex = partition.getBaseIndex();
                            List<Tablet> tablets = baseIndex.getTablets();
                            for (Tablet tablet : tablets) {
                                List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                                for (Replica replica : replicas) {
                                    replica.updateVersionInfo(version, -1, version);
                                }
                            }
                        }
                    }
                }
            }
        };

        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE `test`.`tbl_with_hour_partition` (\n" +
                        "  `k1` datetime,\n" +
                        "  `k2` int,\n" +
                        "  `v1` string\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`, `k2`)\n" +
                        "PARTITION BY RANGE(`k1`)\n" +
                        "(\n" +
                        "PARTITION p2023041015 VALUES [(\"2023-04-10 15:00:00\"), (\"2023-04-10 16:00:00\")),\n" +
                        "PARTITION p2023041016 VALUES [(\"2023-04-10 16:00:00\"), (\"2023-04-10 17:00:00\")),\n" +
                        "PARTITION p2023041017 VALUES [(\"2023-04-10 17:00:00\"), (\"2023-04-10 18:00:00\")),\n" +
                        "PARTITION p2023041018 VALUES [(\"2023-04-10 18:00:00\"), (\"2023-04-10 19:00:00\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");")
                .withMaterializedView("CREATE MATERIALIZED VIEW `mv_with_hour_partiton`\n" +
                        "PARTITION BY (date_trunc('hour', `k1`))\n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "PROPERTIES (\"replication_num\" = \"1\", \"partition_refresh_number\"=\"1\")\n" +
                        "AS\n" +
                        "SELECT \n" +
                        "k1,\n" +
                        "count(DISTINCT `v1`) AS `v` \n" +
                        "FROM `test`.`tbl_with_hour_partition`\n" +
                        "group by k1;");
        starRocksAssert.updateTablePartitionVersion("test", "tbl_with_hour_partition", 2);
        starRocksAssert.refreshMvPartition("REFRESH MATERIALIZED VIEW test.mv_with_hour_partiton \n" +
                "PARTITION START (\"2023-04-10 15:00:00\") END (\"2023-04-10 17:00:00\")");
        MaterializedView mv1 = getMv("test", "mv_with_hour_partiton");
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap1 =
                mv1.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(1, versionMap1.size());
        Set<String> partitions1 = versionMap1.values().iterator().next().keySet();
        Assert.assertEquals(2, partitions1.size());

        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE `test`.`tbl_with_day_partition` (\n" +
                        "  `k1` date,\n" +
                        "  `k2` int,\n" +
                        "  `v1` string\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`, `k2`)\n" +
                        "PARTITION BY RANGE(`k1`)\n" +
                        "(\n" +
                        "PARTITION p20230410 VALUES [(\"2023-04-10\"), (\"2023-04-11\")),\n" +
                        "PARTITION p20230411 VALUES [(\"2023-04-11\"), (\"2023-04-12\")),\n" +
                        "PARTITION p20230412 VALUES [(\"2023-04-12\"), (\"2023-04-13\")),\n" +
                        "PARTITION p20230413 VALUES [(\"2023-04-13\"), (\"2023-04-14\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");")
                .withMaterializedView("CREATE MATERIALIZED VIEW `mv_with_day_partiton`\n" +
                        "PARTITION BY (date_trunc('day', `k1`))\n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "PROPERTIES (\"replication_num\" = \"1\", \"partition_refresh_number\"=\"1\")\n" +
                        "AS\n" +
                        "SELECT \n" +
                        "k1,\n" +
                        "count(DISTINCT `v1`) AS `v` \n" +
                        "FROM `test`.`tbl_with_day_partition`\n" +
                        "group by k1;");
        starRocksAssert.updateTablePartitionVersion("test", "tbl_with_day_partition", 2);
        starRocksAssert.refreshMvPartition("REFRESH MATERIALIZED VIEW test.mv_with_day_partiton \n" +
                "PARTITION START (\"2023-04-10\") END (\"2023-04-13\")");
        MaterializedView mv2 = getMv("test", "mv_with_day_partiton");
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap2 =
                mv2.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(1, versionMap2.size());
        Set<String> partitions2 = versionMap2.values().iterator().next().keySet();
        Assert.assertEquals(3, partitions2.size());
    }

    @Test
    public void testDropBaseTablePartitionRemoveVersionMap() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb(stmt.getTableName().getDb());
                    OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                    for (Partition partition : tbl.getPartitions()) {
                        if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                            long version = partition.getVisibleVersion() + 1;
                            partition.setVisibleVersion(version, System.currentTimeMillis());
                            MaterializedIndex baseIndex = partition.getBaseIndex();
                            List<Tablet> tablets = baseIndex.getTablets();
                            for (Tablet tablet : tablets) {
                                List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                                for (Replica replica : replicas) {
                                    replica.updateVersionInfo(version, -1, version);
                                }
                            }
                        }
                    }
                }
            }
        };

        starRocksAssert.useDatabase("test")
            .withTable("CREATE TABLE `test`.`tbl_with_partition` (\n" +
                "  `k1` date,\n" +
                "  `k2` int,\n" +
                "  `v1` string\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "PARTITION p20230410 VALUES [(\"2023-04-10\"), (\"2023-04-11\")),\n" +
                "PARTITION p20230411 VALUES [(\"2023-04-11\"), (\"2023-04-12\")),\n" +
                "PARTITION p20230412 VALUES [(\"2023-04-12\"), (\"2023-04-13\")),\n" +
                "PARTITION p20230413 VALUES [(\"2023-04-13\"), (\"2023-04-14\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");")
            .withMaterializedView("CREATE MATERIALIZED VIEW `mv_with_partition`\n" +
                "PARTITION BY (date_trunc('day', k1))\n" +
                "REFRESH DEFERRED MANUAL \n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\", \"partition_refresh_number\"=\"1\")\n" +
                "AS\n" +
                "SELECT \n" +
                "k1,\n" +
                "count(DISTINCT `v1`) AS `v` \n" +
                "FROM `test`.`tbl_with_partition`\n" +
                "group by k1;");
        starRocksAssert.updateTablePartitionVersion("test", "tbl_with_partition", 2);
        starRocksAssert.refreshMvPartition("REFRESH MATERIALIZED VIEW test.mv_with_partition \n" +
                "PARTITION START (\"2023-04-10\") END (\"2023-04-14\")");
        MaterializedView mv = getMv("test", "mv_with_partition");
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
            mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(1, versionMap.size());
        Set<String> partitions = versionMap.values().iterator().next().keySet();
        //        Assert.assertEquals(4, partitions.size());

        starRocksAssert.alterMvProperties(
            "alter materialized view test.mv_with_partition set (\"partition_ttl_number\" = \"1\")");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
            .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("mv_with_partition");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();
        starRocksAssert.refreshMvPartition("REFRESH MATERIALIZED VIEW test.mv_with_partition \n" +
                "PARTITION START (\"2023-04-10\") END (\"2023-04-14\")");
        Assert.assertEquals(Sets.newHashSet("p20230413"), versionMap.values().iterator().next().keySet());

        starRocksAssert
            .useDatabase("test")
            .dropMaterializedView("mv_with_partition")
            .dropTable("tbl_with_partition");
    }

    private Set<LocalDate> buildTimePartitions(String tableName, OlapTable tbl, int partitionCount) throws Exception {
        LocalDate currentDate = LocalDate.now();
        Set<LocalDate> partitionBounds = Sets.newHashSet();
        for (int i = 0; i < partitionCount; i++) {
            LocalDate lowerBound = currentDate.minus(Period.ofMonths(i + 1));
            LocalDate upperBound = currentDate.minus(Period.ofMonths(i));
            partitionBounds.add(lowerBound);
            String partitionName = String.format("p_%d_%d", lowerBound.getYear(), lowerBound.getMonthValue());
            String addPartition = String.format("alter table %s add partition p%s values [('%s'), ('%s')) ",
                    tableName, partitionName, lowerBound.toString(), upperBound);
            starRocksAssert.getCtx().executeSql(addPartition);
        }
        return partitionBounds;
    }

    @Test
    public void testMaterializedViewPartitionTTL() throws Exception {
        String dbName = "test";
        String tableName = "test.tbl1";
        starRocksAssert.withTable("CREATE TABLE " + tableName +
                        "(\n" +
                        "    k1 date,\n" +
                        "    v1 int \n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2022-06-01'),\n" +
                        "    PARTITION p2 values less than('2022-07-01'),\n" +
                        "    PARTITION p3 values less than('2022-08-01'),\n" +
                        "    PARTITION p4 values less than('2022-09-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    'replication_num' = '1'\n" +
                        ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.mv_ttl_mv1\n" +
                " REFRESH ASYNC " +
                " PARTITION BY k1\n" +
                " PROPERTIES('partition_ttl'='2 month')" +
                " AS SELECT k1, v1 FROM test.tbl1");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("mv_ttl_mv1");
        Set<LocalDate> addedPartitions = buildTimePartitions(tableName, tbl, 10);

        // Build expectations
        Function<LocalDate, String> formatPartitionName = (dt) ->
                String.format("pp_%d_%d", dt.getYear(), dt.getMonthValue());
        Comparator<LocalDate> cmp = Comparator.comparing(x -> x, Comparator.reverseOrder());
        Function<Integer, Set<String>> expect = (n) -> addedPartitions.stream()
                .sorted(cmp).limit(n)
                .map(formatPartitionName)
                .collect(Collectors.toSet());

        // initial partitions should consider the ttl
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");

        Assert.assertEquals(expect.apply(2), tbl.getPartitionNames());

        // normal ttl
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='2 month')");
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(expect.apply(2), tbl.getPartitionNames());

        // large ttl
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='10 year')");
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 14, tbl.getPartitions().size());

        // tiny ttl
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='1 day')");
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(expect.apply(1), tbl.getPartitionNames());

        // zero ttl
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='0 day')");
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 14, tbl.getPartitions().size());
        Assert.assertEquals("PT0S", tbl.getTableProperty().getPartitionTTL().toString());

        // tiny ttl
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='24 hour')");
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 1, tbl.getPartitions().size());
        Assert.assertEquals(expect.apply(1), tbl.getPartitionNames());

        // the ttl cross two partitions
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='32 day')");
        cluster.runSql(dbName, "refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(expect.apply(2), tbl.getPartitionNames());
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 2, tbl.getPartitions().size());

        // corner cases
        Assert.assertThrows(Exception.class,
                () -> cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='error')"));
        Assert.assertThrows(Exception.class,
                () -> cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='day')"));
        Assert.assertThrows(Exception.class,
                () -> cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='0')"));
        Assert.assertEquals("P32D", tbl.getTableProperty().getPartitionTTL().toString());
        cluster.runSql(dbName, "alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='0 day')");
        Assert.assertEquals("PT0S", tbl.getTableProperty().getPartitionTTL().toString());

    }
}
