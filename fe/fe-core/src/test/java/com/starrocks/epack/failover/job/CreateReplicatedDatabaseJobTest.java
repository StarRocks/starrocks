// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.ReplicatedObjectMeta;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CreateReplicatedDatabaseJobTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public void execute(FailoverGroupJob job) {
                job.execute();
            }
        };
    }

    @Test
    public void testCreateDatabaseJob() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateDatabaseJobGroup " +
                        "INCLUDE_TABLES = test.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        Database database = objectMeta.getDatabaseMetas().values().iterator().next().getDatabase();

        DropReplicatedDatabaseJob dropJob = new DropReplicatedDatabaseJob(failoverGroup, null, null,
                database, true, true);
        dropJob.execute();

        CreateReplicatedDatabaseJob createJob = new CreateReplicatedDatabaseJob(failoverGroup, database,
                null, true);
        createJob.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }
}