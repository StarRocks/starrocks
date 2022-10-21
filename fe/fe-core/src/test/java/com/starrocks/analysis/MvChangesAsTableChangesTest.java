package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * Test mv changes as base table changes
 */
public class MvChangesAsTableChangesTest {

    private static final Logger LOG = LogManager.getLogger(MvChangesAsTableChangesTest.class);

    @BeforeClass
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.enable_experimental_mv = true;

        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testAsyncIfTableTruncate() throws SQLException {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("test", "CREATE TABLE `t2` (\n" +
                "  `k1` date,\n" +
                "  `k2` int,\n" +
                "  `k3` int\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3;");
        cluster.runSql("test", "INSERT INTO t2 VALUES (\"2020-11-10\",3,3)");
        cluster.runSql("test", "CREATE MATERIALIZED VIEW mv_async_table_truncate\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS SELECT k1,k2 from t2;");
        cluster.runSql("test", "truncate table `t2`;");

        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        List<TaskRunStatus> taskRuns = taskManager.showTaskRunStatus(null);

        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            retryCount++;
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            if (taskRuns.size() == 2) {
                break;
            }
            LOG.info("testAsyncIfTableTruncate is waiting for TaskRunState retryCount:" + retryCount);
        }
        // create mv add one + truncate table add one

        Assert.assertEquals(2, taskRuns.size());
    }
}
