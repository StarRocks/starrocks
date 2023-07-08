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
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
                        "as select k1, k2, v1  from tbl_with_mv;")
                .withMaterializedView("create materialized view mv_with_mv_rewrite_staleness\n" +
                        "PARTITION BY k1\n"+
                        "distributed by hash(k2) buckets 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"" +
                        ")" +
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
            Assert.assertEquals("Not support refresh by partition for single partition mv.", e.getMessage());
        }

        try {
            sql = "REFRESH MATERIALIZED VIEW test.mv2_to_refresh PARTITION START('2022-02-03') END ('2020-02-25') FORCE;";
            statement = (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Batch build partition start date should less than end date.", e.getMessage());
        }

        try {
            sql = "REFRESH MATERIALIZED VIEW test.mv2_to_refresh PARTITION START('dhdfghg') END ('2020-02-25') FORCE;";
            statement = (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Batch build partition EVERY is date type but START or END does not type match.", e.getMessage());
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
        Set<String> partitionsToRefresh1 = mv1.getPartitionNamesToRefreshForMv();
        Assert.assertTrue(partitionsToRefresh1.isEmpty());
        refreshMaterializedView("test", "mv2_to_refresh");
        MaterializedView mv2 = getMv("test", "mv2_to_refresh");
        Set<String> partitionsToRefresh2 = mv2.getPartitionNamesToRefreshForMv();
        Assert.assertTrue(partitionsToRefresh2.isEmpty());
        cluster.runSql("test", "insert into tbl_with_mv partition(p2) values(\"2022-02-20\", 2, 10)");
        OlapTable table = (OlapTable) getTable("test", "tbl_with_mv");
        Partition p1 = table.getPartition("p1");
        Partition p2 = table.getPartition("p2");
        if (p2.getVisibleVersion() == 3) {
            partitionsToRefresh1 = mv1.getPartitionNamesToRefreshForMv();
            Assert.assertEquals(Sets.newHashSet("mv_to_refresh"), partitionsToRefresh1);
            partitionsToRefresh2 = mv2.getPartitionNamesToRefreshForMv();
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
    public void testMaxMVRewriteStaleness() throws Exception {
        Set<String> cachePartitionsToRefresh;

        // refresh partitions are not empty if base table is updated.
        cluster.runSql("test", "insert into tbl_with_mv values(\"2022-02-20\", 1, 10)");
        // no refresh partitions if there is new data & refresh.
        {
            refreshMaterializedView("test", "mv_with_mv_rewrite_staleness");
            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
            cachePartitionsToRefresh = mv1.getPartitionNamesToRefreshForMv();
            Assert.assertTrue(cachePartitionsToRefresh.isEmpty());
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
        cluster.runSql("test", "insert into tbl_with_mv values(\"2022-02-20\", 1, 10)");
        {
            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
            Set<String> partitionsToRefresh = mv1.getPartitionNamesToRefreshForMv();
            Assert.assertTrue(partitionsToRefresh.isEmpty());

        }
        // no refresh partitions if there is no new data.
        {
            refreshMaterializedView("test", "mv_with_mv_rewrite_staleness");
            MaterializedView mv2 = getMv("test", "mv_with_mv_rewrite_staleness");
            Set<String> partitionsToRefresh = mv2.getPartitionNamesToRefreshForMv();
            Assert.assertTrue(partitionsToRefresh.isEmpty());
            Assert.assertEquals(cachePartitionsToRefresh, partitionsToRefresh);
        }
        // no refresh partitions if there is new data & no refresh but is set `mv_rewrite_staleness`.
        {
            cluster.runSql("test", "insert into tbl_with_mv values(\"2022-02-22\", 1, 10)");
            MaterializedView mv1 = getMv("test", "mv_with_mv_rewrite_staleness");
            Set<String> partitionsToRefresh = mv1.getPartitionNamesToRefreshForMv();
            Assert.assertTrue(partitionsToRefresh.isEmpty());
            Assert.assertEquals(cachePartitionsToRefresh, partitionsToRefresh);
        }
    }
}
