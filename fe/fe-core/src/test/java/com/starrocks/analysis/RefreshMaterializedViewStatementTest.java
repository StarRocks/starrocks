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
        Config.alter_scheduler_interval_millisecond = 100;
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
            Assert.assertEquals("Getting analyzing error at line 1, column 26. Detail message: " +
                    "Can not find materialized view:no_exists.", e.getMessage());
        }
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        cluster.runSql("test",
                "create table table_name_tmp_1 ( c1 bigint NOT NULL, c2 string not null, c3 int not null ) " +
                        " DISTRIBUTED BY HASH(c1) BUCKETS 1 " +
                        " PROPERTIES(\"replication_num\" = \"1\");");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        starRocksAssert.withMaterializedView("create materialized view mv1 distributed by hash(`c1`) " +
                " refresh manual" +
                " as select c1, sum(c3) as total from table_name_tmp_1 group by c1");
        cluster.runSql("test", "insert into table_name_tmp_1 values(1, \"str1\", 100)");
        Table table = db.getTable("table_name_tmp_1");
        Assert.assertNotNull(table);
        Table t2 = db.getTable("mv1");
        Assert.assertNotNull(t2);
        MaterializedView mv1 = (MaterializedView) t2;
        cluster.runSql("test", "refresh materialized view mv1 with sync mode");

        MaterializedView.MvRefreshScheme refreshScheme = mv1.getRefreshScheme();
        Assert.assertNotNull(refreshScheme);
        System.out.println("visibleVersionMap:" + refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap());
        Assert.assertTrue(refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().containsKey(table.getId()));
        Map<String, MaterializedView.BasePartitionInfo> partitionInfoMap =
                refreshScheme.getAsyncRefreshContext().getBaseTableVisibleVersionMap().get(table.getId());
        if (partitionInfoMap.containsKey("table_name_tmp_1")) {
            MaterializedView.BasePartitionInfo partitionInfo = partitionInfoMap.get("table_name_tmp_1");
            Assert.assertEquals(table.getPartition("table_name_tmp_1").getVisibleVersion(), partitionInfo.getVersion());
        }
    }
}
