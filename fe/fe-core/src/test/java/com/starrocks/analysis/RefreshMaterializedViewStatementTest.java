// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

<<<<<<< HEAD
=======
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
>>>>>>> 6b3db26f0 ([Enhancement] fix disk space occupation problems after insert overwrite (#16133))
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
<<<<<<< HEAD
=======
import org.junit.AfterClass;
>>>>>>> 6b3db26f0 ([Enhancement] fix disk space occupation problems after insert overwrite (#16133))
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RefreshMaterializedViewStatementTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
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

<<<<<<< HEAD
=======
    @Test
    public void testRefreshMaterializedView() throws Exception {
        cluster.runSql("test",
                "create table table_name_tmp_1 ( c1 bigint NOT NULL, c2 string not null, c3 int not null ) " +
                        " DISTRIBUTED BY HASH(c1) BUCKETS 1 " +
                        " PROPERTIES(\"replication_num\" = \"1\");");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        starRocksAssert.withNewMaterializedView("create materialized view mv1 distributed by hash(`c1`) " +
                " refresh manual" +
                " as select c1, sum(c3) as total from table_name_tmp_1 group by c1");
        cluster.runSql("test", "insert into table_name_tmp_1 values(1, \"str1\", 100)");
        Table table = db.getTable("table_name_tmp_1");
        Assert.assertNotNull(table);
        Table t2 = db.getTable("mv1");
        Assert.assertNotNull(t2);
        MaterializedView mv1 = (MaterializedView) t2;

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        final String mvTaskName = TaskBuilder.getMvTaskName(mv1.getId());
        if (!taskManager.containTask(mvTaskName)) {
            Task task = TaskBuilder.buildMvTask(mv1, "test");
            TaskBuilder.updateTaskInfo(task, mv1);
            taskManager.createTask(task, false);
        }
        taskManager.executeTaskSync(mvTaskName);
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
>>>>>>> 6b3db26f0 ([Enhancement] fix disk space occupation problems after insert overwrite (#16133))
}
