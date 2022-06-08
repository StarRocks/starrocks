// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DropTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeDropTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testDropTable() {
        analyzeSuccess("drop table if exists test1 force");
        analyzeSuccess("drop table if exists test1");
        analyzeSuccess("drop table test1 force");
        analyzeSuccess("drop table db1.test1");
        analyzeFail("drop table exists test1");
        DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop table if exists db1.test1 force");
        Assert.assertEquals("default_cluster:db1", stmt.getDbName());
        Assert.assertEquals("test1", stmt.getTableName());
        Assert.assertTrue(stmt.isSetIfExists());
        Assert.assertTrue(stmt.isForceDrop());
        stmt = (DropTableStmt) analyzeSuccess("drop table test2");
        Assert.assertEquals("default_cluster:test", stmt.getDbName());
        Assert.assertEquals("test2", stmt.getTableName());
        Assert.assertFalse(stmt.isSetIfExists());
        Assert.assertFalse(stmt.isForceDrop());
    }

    @Test
    public void testDropView() {
        analyzeSuccess("drop view if exists view");
        analyzeSuccess("drop view test1");
        analyzeSuccess("drop view db1.test1");
        analyzeFail("drop view test1 force");
        analyzeFail("drop view exists test1");
        DropTableStmt stmt = (DropTableStmt) analyzeSuccess("drop view if exists db1.test1");
        Assert.assertEquals("default_cluster:db1", stmt.getDbName());
        Assert.assertEquals("test1", stmt.getTableName());
        Assert.assertTrue(stmt.isView());
        Assert.assertTrue(stmt.isSetIfExists());
        Assert.assertFalse(stmt.isForceDrop());
    }

}