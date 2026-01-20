// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ExcludeObjectMgrTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        String sql = "create table ExcludeObjectMgrTestTable (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));
    }

    @Test
    public void testCatalogs() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP ExcludeObjectMgrTestGroup " +
                        "INCLUDE_TABLES = default_catalog.*.* " +
                        "EXCLUDE_TABLES = default_catalog.*.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        ExcludeObjectMgr excludeMgr = new ExcludeObjectMgr(stmt);
        Assert.assertTrue(excludeMgr.isExcludeCatalog("default_catalog"));
        Assert.assertTrue(excludeMgr.isExcludeDatabase("default_catalog", "test"));
        Assert.assertTrue(excludeMgr.isExcludeTable("default_catalog", "test", "ExcludeObjectMgrTestTable"));
    }

    @Test
    public void testDatabases() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP ExcludeObjectMgrTestGroup " +
                        "INCLUDE_TABLES = default_catalog.*.* " +
                        "EXCLUDE_TABLES = test.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        ExcludeObjectMgr excludeMgr = new ExcludeObjectMgr(stmt);
        Assert.assertTrue(excludeMgr.isExcludeDatabase("default_catalog", "test"));
    }

    @Test
    public void testTables() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP ExcludeObjectMgrTestGroup " +
                        "INCLUDE_TABLES = default_catalog.*.* " +
                        "EXCLUDE_TABLES = test.ExcludeObjectMgrTestTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        ExcludeObjectMgr excludeMgr = new ExcludeObjectMgr(stmt);
        Assert.assertTrue(excludeMgr.isExcludeTable("default_catalog", "test", "ExcludeObjectMgrTestTable"));
    }
}
