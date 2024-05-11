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

public class ReplicatedObjectMgrTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        String sql = "create table ReplicatedObjectMgrTestTable (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));
    }

    @Test
    public void testCatalogs() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP ReplicatedObjectMgrTestGroup " +
                        "CATALOGS = default_catalog " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        ReplicatedObjectMgr objectMgr = new ReplicatedObjectMgr(stmt);

        Assert.assertEquals(1, objectMgr.getCatalogs().size());
        Assert.assertEquals(0, objectMgr.getDatabases().size());
        Assert.assertEquals(0, objectMgr.getTables().size());

        ReplicatedObjectMeta objectMeta = objectMgr.toObjectMeta(null);

        Assert.assertEquals(1, objectMeta.getCatalogMetas().size());
        Assert.assertEquals(0, objectMeta.getDatabaseMetas().size());
        Assert.assertEquals(0, objectMeta.getTableMetas().size());
    }

    @Test
    public void testDatabases() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP ReplicatedObjectMgrTestGroup " +
                        "DATABASES = test " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        ReplicatedObjectMgr objectMgr = new ReplicatedObjectMgr(stmt);

        Assert.assertEquals(0, objectMgr.getCatalogs().size());
        Assert.assertEquals(1, objectMgr.getDatabases().size());
        Assert.assertEquals(0, objectMgr.getTables().size());

        ReplicatedObjectMeta objectMeta = objectMgr.toObjectMeta(null);

        Assert.assertEquals(0, objectMeta.getCatalogMetas().size());
        Assert.assertEquals(1, objectMeta.getDatabaseMetas().size());
        Assert.assertEquals(0, objectMeta.getTableMetas().size());
    }

    @Test
    public void testTables() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP ReplicatedObjectMgrTestGroup " +
                        "TABLES = test.ReplicatedObjectMgrTestTable " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        ReplicatedObjectMgr objectMgr = new ReplicatedObjectMgr(stmt);

        Assert.assertEquals(0, objectMgr.getCatalogs().size());
        Assert.assertEquals(0, objectMgr.getDatabases().size());
        Assert.assertEquals(1, objectMgr.getTables().size());

        ReplicatedObjectMeta objectMeta = objectMgr.toObjectMeta(null);

        Assert.assertEquals(0, objectMeta.getCatalogMetas().size());
        Assert.assertEquals(0, objectMeta.getDatabaseMetas().size());
        Assert.assertEquals(1, objectMeta.getTableMetas().size());
    }
}
