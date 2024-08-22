// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.ReplicatedObjectMeta;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CheckReplicatedObjectMetaJobTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        String sql = "create table CheckReplicatedObjectMetaJobTestTable (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public void execute(FailoverGroupJob job) {
                job.execute();
            }
        };
    }

    @Test
    public void testCheckCatalogExisted() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCheckCatalogExistedGroup " +
                        "INCLUDE_TABLES = default_catalog.*.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");
        new MockUp<FailoverGroup>() {
            @Mock
            public ReplicatedObjectMeta getObjectMeta() {
                return objectMeta;
            }
        };

        CheckReplicatedObjectMetaJob job = new CheckReplicatedObjectMetaJob(failoverGroup);
        job.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCheckDatabaseExisted() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCheckDatabaseExistedGroup " +
                        "INCLUDE_TABLES = test.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");
        new MockUp<FailoverGroup>() {
            @Mock
            public ReplicatedObjectMeta getObjectMeta() {
                return objectMeta;
            }
        };

        CheckReplicatedObjectMetaJob job = new CheckReplicatedObjectMetaJob(failoverGroup);
        job.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCheckTableExisted() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCheckTableExistedGroup " +
                        "INCLUDE_TABLES = test.CheckReplicatedObjectMetaJobTestTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");
        new MockUp<FailoverGroup>() {
            @Mock
            public ReplicatedObjectMeta getObjectMeta() {
                return objectMeta;
            }
        };

        CheckReplicatedObjectMetaJob job = new CheckReplicatedObjectMetaJob(failoverGroup);
        job.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }
}
