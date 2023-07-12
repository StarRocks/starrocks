// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class FailoverGroupAnalyzerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAnalyzeCreatePrimaryFailoverGroup() {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group " +
                    "CATALOGS = test_catalog " +
                    "DATABASES = test_db, test_catalog.test_db " +
                    "TABLES = test_table, test_db.test_table, test_catalog.test_db.test_table " +
                    "MEMBERS = " +
                        "'test_member1:SELF'," +
                        "'test_member2:192.168.0.1:9090'" +
                    "SCHEDULE = '1h'");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());

        Assert.assertNotNull(stmt.getCatalogNames());
        Assert.assertEquals(1, stmt.getCatalogNames().size());
        Assert.assertEquals("test_catalog", stmt.getCatalogNames().get(0));

        Assert.assertNotNull(stmt.getDatabaseNames());
        Assert.assertEquals(2, stmt.getDatabaseNames().size());
        Assert.assertEquals("default_catalog", stmt.getDatabaseNames().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getDatabaseNames().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getDatabaseNames().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getDatabaseNames().get(1).getDatabase());

        Assert.assertNotNull(stmt.getTableNames());
        Assert.assertEquals(3, stmt.getTableNames().size());
        Assert.assertEquals("default_catalog", stmt.getTableNames().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getTableNames().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getTableNames().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getTableNames().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getTableNames().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getTableNames().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getTableNames().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getTableNames().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getTableNames().get(2).getTbl());

        Assert.assertNotNull(stmt.getMembers());
        Assert.assertEquals(2, stmt.getMembers().size());
        Assert.assertEquals("test_member1:SELF", stmt.getMembers().get(0));
        Assert.assertEquals("test_member2:192.168.0.1:9090", stmt.getMembers().get(1));

        Assert.assertNotNull(stmt.getSchedule());
        Assert.assertEquals("1h", stmt.getSchedule());

        analyzeFail(
                "CREATE FAILOVER GROUP test_group " +
                    "MEMBERS = " +
                        "'test_member1:SELF', ''" +
                    "SCHEDULE = '1h'",
                "Member is empty");
    }

    @Test
    public void testAnalyzeCreateSecondaryFailoverGroup() {
        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group " +
                    "AS REPLICA OF '192.168.0.1:9090'");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());
        Assert.assertEquals("192.168.0.1:9090", stmt.getPrimaryMember());

        analyzeFail(
                "CREATE FAILOVER GROUP test_group " +
                    "AS REPLICA OF ''",
                "Primary member is empty");
    }
}
