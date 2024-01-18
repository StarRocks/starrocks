// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class RefreshMaterializedViewStatementTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static PseudoCluster cluster;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();

        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "REFRESH MATERIALIZED VIEW no_exists;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.assertEquals("Can not find materialized view:no_exists", e.getMessage());
        }
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        cluster.runSql("test",
                "create table t1 ( c1 bigint NOT NULL, c2 string not null, c3 int not null ) " +
                        " DISTRIBUTED BY HASH(c1) BUCKETS 1 " +
                        " PROPERTIES(\"replication_num\" = \"1\");");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        starRocksAssert.withMaterializedView("create materialized view mv1 distributed by hash(`c1`) " +
                " refresh manual" +
                " as select c1, sum(c3) as total from t1 group by c1");
        cluster.runSql("test", "insert into t1 values(1, \"str1\", 100)");
        Table t1 = db.getTable("t1");
        Assert.assertNotNull(t1);
        Table t2 = db.getTable("mv1");
        Assert.assertNotNull(t2);
        MaterializedView mv1 = (MaterializedView) t2;
        cluster.runSql("test", "refresh materialized view mv1 with sync mode");

        MaterializedView.MvRefreshScheme refreshScheme = mv1.getRefreshScheme();
        Assert.assertNotNull(refreshScheme);
        Assert.assertTrue(refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().containsKey(t1.getId()));
        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap =
                refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().get(t1.getId());
        Assert.assertTrue(partitionInfoMap.containsKey("t1"));
        MaterializedView.BasePartitionInfo partitionInfo = partitionInfoMap.get("t1");
        Assert.assertEquals(t1.getPartition("t1").getVisibleVersion(), partitionInfo.getVersion());
    }
}
