// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

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
                        "INCLUDE_TABLES = default_catalog.*.* " +
                        "EXCLUDE_TABLES = not_existed_db.* " +
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

    @Test
    public void testSecondaryReadonlyIncludeExclude() throws Exception {
        starRocksAssert.withTable(
                "CREATE TABLE IF NOT EXISTS test.fg_ro_t1(k1 int) " +
                        "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        starRocksAssert.withTable(
                "CREATE TABLE IF NOT EXISTS test.fg_ro_t2(k1 int) " +
                        "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Assert.assertNotNull(db);
        Table table1 = db.getTable("fg_ro_t1");
        Table table2 = db.getTable("fg_ro_t2");
        Assert.assertNotNull(table1);
        Assert.assertNotNull(table2);

        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP fg_ro_group_1 AS REPLICA OF '192.168.0.1:9090'");
        FailoverGroupMgr failoverGroupMgr = new FailoverGroupMgr();
        failoverGroupMgr.createFailoverGroup(stmt);

        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup("fg_ro_group_1");
        failoverGroup.getIncludeMgr().addIncludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, db.getId());
        failoverGroup.getExcludeMgr().addExcludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                db.getFullName(), table2.getName());

        Assert.assertTrue(failoverGroupMgr.isSecondaryReadonly(db.getId(), Arrays.asList(table1.getId())));
        Assert.assertFalse(failoverGroupMgr.isSecondaryReadonly(db.getId(), Arrays.asList(table2.getId())));
        Assert.assertTrue(failoverGroupMgr.isSecondaryReadonly(
                db.getId(), Arrays.asList(table1.getId(), table2.getId())));
    }

    @Test
    public void testSecondaryReadonlyIncludeTable() throws Exception {
        starRocksAssert.withTable(
                "CREATE TABLE IF NOT EXISTS test.fg_ro_t3(k1 int) " +
                        "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        starRocksAssert.withTable(
                "CREATE TABLE IF NOT EXISTS test.fg_ro_t4(k1 int) " +
                        "distributed by hash(k1) buckets 1 properties('replication_num' = '1');");

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Assert.assertNotNull(db);
        Table table3 = db.getTable("fg_ro_t3");
        Table table4 = db.getTable("fg_ro_t4");
        Assert.assertNotNull(table3);
        Assert.assertNotNull(table4);

        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP fg_ro_group_2 AS REPLICA OF '192.168.0.1:9090'");
        FailoverGroupMgr failoverGroupMgr = new FailoverGroupMgr();
        failoverGroupMgr.createFailoverGroup(stmt);

        FailoverGroup failoverGroup = failoverGroupMgr.getFailoverGroup("fg_ro_group_2");
        failoverGroup.getIncludeMgr().addIncludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, db.getId(),
                table4.getId());

        Assert.assertFalse(failoverGroupMgr.isSecondaryReadonly(db.getId(), Arrays.asList(table3.getId())));
        Assert.assertTrue(failoverGroupMgr.isSecondaryReadonly(db.getId(), Arrays.asList(table4.getId())));
    }
}
