// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class FailoverGroupTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testPrimary() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testPrimaryFailoverGroup " +
                        "CATALOGS = default_catalog " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroupMgr failoverGroupMgr = new FailoverGroupMgr();
        failoverGroupMgr.createFailoverGroup(stmt);

        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup("testPrimaryFailoverGroup");
        Assert.assertNotNull(failoverGroup);
    }

    @Test
    public void testSecondary() throws Exception {
        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testSecondaryFailoverGroup " +
                        "AS REPLICA OF '192.168.0.1:9090'");

        FailoverGroupMgr failoverGroupMgr = new FailoverGroupMgr();
        failoverGroupMgr.createFailoverGroup(stmt);

        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup("testSecondaryFailoverGroup");
        Assert.assertNotNull(failoverGroup);
    }
}
