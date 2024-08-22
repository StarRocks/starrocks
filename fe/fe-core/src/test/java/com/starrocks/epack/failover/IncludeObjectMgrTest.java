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

public class IncludeObjectMgrTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        String sql = "create table IncludeObjectMgrTestTable (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));
    }

    @Test
    public void testCatalogs() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP IncludeObjectMgrTestGroup " +
                        "INCLUDE_TABLES = default_catalog.*.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        IncludeObjectMgr includeMgr = new IncludeObjectMgr(stmt);

        Assert.assertEquals(1, includeMgr.getIncludeCatalogs().size());
        Assert.assertEquals(0, includeMgr.getIncludeDatabases().size());
        Assert.assertEquals(0, includeMgr.getIncludeTables().size());

        ReplicatedObjectMeta objectMeta = includeMgr.toObjectMeta(null);

        Assert.assertEquals(1, objectMeta.getCatalogMetas().size());
        Assert.assertEquals(0, objectMeta.getDatabaseMetas().size());
        Assert.assertEquals(0, objectMeta.getTableMetas().size());
    }

    @Test
    public void testDatabases() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP IncludeObjectMgrTestGroup " +
                        "INCLUDE_TABLES = test.* " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        IncludeObjectMgr includeMgr = new IncludeObjectMgr(stmt);

        Assert.assertEquals(0, includeMgr.getIncludeCatalogs().size());
        Assert.assertEquals(1, includeMgr.getIncludeDatabases().size());
        Assert.assertEquals(0, includeMgr.getIncludeTables().size());

        ReplicatedObjectMeta objectMeta = includeMgr.toObjectMeta(null);

        Assert.assertEquals(0, objectMeta.getCatalogMetas().size());
        Assert.assertEquals(1, objectMeta.getDatabaseMetas().size());
        Assert.assertEquals(0, objectMeta.getTableMetas().size());
    }

    @Test
    public void testTables() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP IncludeObjectMgrTestGroup " +
                        "INCLUDE_TABLES = test.IncludeObjectMgrTestTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        IncludeObjectMgr includeMgr = new IncludeObjectMgr(stmt);

        Assert.assertEquals(0, includeMgr.getIncludeCatalogs().size());
        Assert.assertEquals(0, includeMgr.getIncludeDatabases().size());
        Assert.assertEquals(1, includeMgr.getIncludeTables().size());

        ReplicatedObjectMeta objectMeta = includeMgr.toObjectMeta(null);

        Assert.assertEquals(0, objectMeta.getCatalogMetas().size());
        Assert.assertEquals(0, objectMeta.getDatabaseMetas().size());
        Assert.assertEquals(1, objectMeta.getTableMetas().size());
    }
}
