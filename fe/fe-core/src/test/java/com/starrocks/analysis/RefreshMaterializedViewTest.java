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
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RefreshMaterializedViewTest {
    private static ConnectContext connectContext;
    private static PseudoCluster cluster;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");

        Config.enable_experimental_mv = true;
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

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
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

    private MaterializedView getMv(String dbName, String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(mvName);
        Assert.assertNotNull(table);
        Assert.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        return mv;
    }

    private Table getTable(String dbName, String tableName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        Table table = db.getTable(tableName);
        Assert.assertNotNull(table);
        return table;
    }

    private void refreshMaterializedView(String dbName, String mvName) throws Exception {
        cluster.runSql(dbName, String.format("refresh materialized view %s with sync mode", mvName));
    }

    @Test
    public void testRefreshExecution() throws Exception {
        cluster.runSql("test", "insert into tbl_with_mv values(\"2022-02-20\", 1, 10)");
        refreshMaterializedView("test", "mv_to_refresh");
        MaterializedView mv1 = getMv("test", "mv_to_refresh");
        Set<String> partitionsToRefresh1 = mv1.getMVToRefreshPartitionNames();
        Assert.assertTrue(partitionsToRefresh1.isEmpty());
        refreshMaterializedView("test", "mv2_to_refresh");
        MaterializedView mv2 = getMv("test", "mv2_to_refresh");
        Set<String> partitionsToRefresh2 = mv2.getMVToRefreshPartitionNames();
        Assert.assertTrue(partitionsToRefresh2.isEmpty());
        cluster.runSql("test", "insert into tbl_with_mv partition(p2) values(\"2022-02-20\", 2, 10)");
        OlapTable table = (OlapTable) getTable("test", "tbl_with_mv");
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        if (p2.getVisibleVersion() == 3) {
            partitionsToRefresh1 = mv1.getMVToRefreshPartitionNames();
            Assert.assertEquals(Sets.newHashSet("mv_to_refresh"), partitionsToRefresh1);
            partitionsToRefresh2 = mv2.getMVToRefreshPartitionNames();
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
    public void testMaxMVRewriteStaleness1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tbl_staleness1 \n" +
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
                        "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness\n" +
                "PARTITION BY k1\n"+
                "distributed by hash(k2) buckets 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ")" +
                "refresh manual\n" +
                "as select k1, k2, v1  from tbl_staleness1;");

        // refresh partitions are not empty if base table is updated.
        cluster.runSql("test", "insert into tbl_staleness1 partition(p2) values(\"2022-02-20\", 1, 10)");
        {
            refreshMaterializedView("test", "mv_with_mv_rewrite_staleness");
        }

        // alter mv_rewrite_staleness
        {
            String alterMvSql = "alter materialized view mv_with_mv_rewrite_staleness " +
                    "set (\"mv_rewrite_staleness_second\" = \"60\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            GlobalStateMgr.getCurrentState().alterMaterializedView(stmt);
        }
        // no refresh partitions if mv_rewrite_staleness is set.
        cluster.runSql("test", "insert into tbl_staleness1 values(\"2022-02-20\", 1, 10)");
        {
            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
            Set<String> partitionsToRefresh = mv1.getMVToRefreshPartitionNames();
            Assert.assertTrue(partitionsToRefresh.isEmpty());

        }
        // no refresh partitions if there is no new data.
        {
            refreshMaterializedView("test", "mv_with_mv_rewrite_staleness");
            MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness");
            Set<String> partitionsToRefresh = mv2.getMVToRefreshPartitionNames();
            Assert.assertTrue(partitionsToRefresh.isEmpty());
        }
        // no refresh partitions if there is new data & no refresh but is set `mv_rewrite_staleness`.
        {
            cluster.runSql("test", "insert into tbl_staleness1 values(\"2022-02-22\", 1, 10)");
            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
            Set<String> partitionsToRefresh = mv1.getMVToRefreshPartitionNames();
            Assert.assertTrue(partitionsToRefresh.isEmpty());
        }
        starRocksAssert.dropTable("tbl_staleness1");
        starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness");
    }

    @Test
    public void testMaxMVRewriteStaleness2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tbl_staleness2 \n" +
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
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness2 \n" +
                "PARTITION BY k1\n"+
                "distributed by hash(k2) buckets 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"," +
                "\"mv_rewrite_staleness_second\" = \"60\"" +
                ")" +
                "refresh manual\n" +
                "as select k1, k2, v1  from tbl_staleness2;");

        // refresh partitions are not empty if base table is updated.
        {
            cluster.runSql("test", "insert into tbl_staleness2 partition(p2) values(\"2022-02-20\", 1, 10)");
            refreshMaterializedView("test", "mv_with_mv_rewrite_staleness2");

            Table tbl1 = getTable("test", "tbl_staleness2");
            Optional<Long> maxPartitionRefreshTimestamp =
                    tbl1.getPartitions().stream().map(Partition::getVisibleVersionTime).max(Long::compareTo);
            Assert.assertTrue(maxPartitionRefreshTimestamp.isPresent());

            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness2");
            Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());
            Assert.assertEquals(mv1.maxBaseTableRefreshTimestamp().get(), maxPartitionRefreshTimestamp.get());

            long mvRefreshTimeStamp = mv1.getLastRefreshTime();
            Assert.assertTrue(mvRefreshTimeStamp ==  maxPartitionRefreshTimestamp.get());
            Assert.assertTrue(mv1.isStalenessSatisfied());
        }

        // no refresh partitions if mv_rewrite_staleness is set.
        {
            cluster.runSql("test", "insert into tbl_staleness2 values(\"2022-02-20\", 2, 10)");

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
            Assert.assertTrue((tblMaxPartitionRefreshTimestamp - mvRefreshTimeStamp) / 1000 < 60);
            Assert.assertTrue(mv1.isStalenessSatisfied());

            Set<String> partitionsToRefresh = mv1.getMVToRefreshPartitionNames();
            Assert.assertTrue(partitionsToRefresh.isEmpty());
        }
        starRocksAssert.dropTable("tbl_staleness2");
        starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness2");
    }

    @Test
    public void testMaxMVRewriteStaleness3() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tbl_staleness3 \n" +
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
                "PROPERTIES('replication_num' = '1');");

        starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness21 \n" +
                "PARTITION BY k1\n"+
                "distributed by hash(k2) buckets 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"," +
                "\"mv_rewrite_staleness_second\" = \"60\"" +
                ")" +
                "refresh manual\n" +
                "as select k1, k2, v1  from tbl_staleness3;");

        starRocksAssert.withMaterializedView("create materialized view mv_with_mv_rewrite_staleness22 \n" +
                "PARTITION BY k1\n"+
                "distributed by hash(k2) buckets 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"," +
                "\"mv_rewrite_staleness_second\" = \"60\"" +
                ")" +
                "refresh manual\n" +
                "as select k1, k2, count(1)  from mv_with_mv_rewrite_staleness21 group by k1, k2;");

        {
            cluster.runSql("test", "insert into tbl_staleness3 partition(p2) values(\"2022-02-20\", 1, 10)");
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
                Assert.assertTrue(mvRefreshTimeStamp ==  maxPartitionRefreshTimestamp.get());
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
                Assert.assertTrue(mvRefreshTimeStamp ==  maxPartitionRefreshTimestamp.get());
                Assert.assertTrue(mv2.isStalenessSatisfied());
            }
        }

        {
            cluster.runSql("test", "insert into tbl_staleness3 values(\"2022-02-20\", 2, 10)");
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
                Assert.assertTrue((tblMaxPartitionRefreshTimestamp - mvRefreshTimeStamp) / 1000 < 60);
                Assert.assertTrue(mv1.isStalenessSatisfied());

                Set<String> partitionsToRefresh = mv1.getMVToRefreshPartitionNames();
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
                Assert.assertTrue((tblMaxPartitionRefreshTimestamp - mvRefreshTimeStamp) / 1000 < 60);
                Assert.assertTrue(mv2.isStalenessSatisfied());

                Set<String> partitionsToRefresh = mv2.getMVToRefreshPartitionNames();
                Assert.assertTrue(partitionsToRefresh.isEmpty());
            }
        }

        {
            cluster.runSql("test", "insert into tbl_staleness3 values(\"2022-02-20\", 2, 10)");
            {
                // alter mv_rewrite_staleness
                {
                    String alterMvSql = "alter materialized view mv_with_mv_rewrite_staleness21 " +
                            "set (\"mv_rewrite_staleness_second\" = \"0\")";
                    AlterMaterializedViewStmt stmt =
                            (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
                    GlobalStateMgr.getCurrentState().alterMaterializedView(stmt);
                }

                MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness21");
                Assert.assertFalse(mv1.isStalenessSatisfied());
                Assert.assertTrue(mv1.maxBaseTableRefreshTimestamp().isPresent());

                MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness22");
                Assert.assertFalse(mv1.isStalenessSatisfied());
                Assert.assertFalse(mv2.maxBaseTableRefreshTimestamp().isPresent());

                Set<String> partitionsToRefresh = mv2.getMVToRefreshPartitionNames();
                Assert.assertFalse(partitionsToRefresh.isEmpty());
            }
        }
        starRocksAssert.dropTable("tbl_staleness3");
        starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness21");
        starRocksAssert.dropMaterializedView("mv_with_mv_rewrite_staleness22");
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
                "PARTITION START (\"2023-04-10 15:00:00\") END (\"2023-04-10 17:00:00\");");
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
                "PARTITION START (\"2023-04-10\") END (\"2023-04-13\");");
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
            "PARTITION START (\"2023-04-10\") END (\"2023-04-14\");");
        MaterializedView mv = getMv("test", "mv_with_partition");
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
            mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(1, versionMap.size());
        Set<String> partitions = versionMap.values().iterator().next().keySet();
        Assert.assertEquals(4, partitions.size());

        starRocksAssert.alterMvProperties(
            "alter materialized view test.mv_with_partition set (\"partition_ttl_number\" = \"1\")");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
            .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("mv_with_partition");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();
        Assert.assertEquals(Sets.newHashSet("p20230413"), versionMap.values().iterator().next().keySet());

        starRocksAssert
            .useDatabase("test")
            .dropMaterializedView("mv_with_partition")
            .dropTable("tbl_with_partition");
    }
}
