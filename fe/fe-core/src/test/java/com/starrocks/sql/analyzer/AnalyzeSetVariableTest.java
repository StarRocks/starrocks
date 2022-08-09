// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.SetPassVar;
import com.starrocks.analysis.SetStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeSetVariableTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSetVariable() {
        String sql = "set query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set `query_timeout` = 10";
        analyzeSuccess(sql);
        sql = "set \"query_timeout\" = 10";
        analyzeFail(sql);
        sql = "set GLOBAL query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set SESSION query_timeout = 10";
        analyzeSuccess(sql);
        sql = "set LOCAL query_timeout = 10";
        analyzeSuccess(sql);
    }

    @Test
    public void testUserVariable() {
        String sql = "set @var1 = 1";
        analyzeSuccess(sql);
        sql = "set @`var1` = 1";
        analyzeSuccess(sql);
        sql = "set @'var1' = 1";
        analyzeSuccess(sql);
        sql = "set @\"var1\" = 1";
        analyzeSuccess(sql);
    }

    @Test
    public void testSystemVariable() {
        String sql = "set @@query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@GLOBAL.query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@SESSION.query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@LOCAL.query_timeout = 1";
        analyzeSuccess(sql);
        sql = "set @@event_scheduler = ON";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetNames() {
        String sql = "SET NAMES 'utf8mb4' COLLATE 'bogus'";
        analyzeSuccess(sql);
        sql = "SET NAMES 'utf8mb4'";
        analyzeSuccess(sql);
        sql = "SET NAMES default";
        analyzeSuccess(sql);
        sql = "SET CHARSET 'utf8mb4'";
        analyzeSuccess(sql);
        sql = "SET CHAR SET 'utf8mb4'";
        analyzeSuccess(sql);
    }

    @Test
    public void testSetPassword() {
        String sql = "SET PASSWORD FOR 'testUser' = PASSWORD('testPass')";
        SetStmt setStmt = (SetStmt) analyzeSuccess(sql);
        SetPassVar setPassVar = (SetPassVar) setStmt.getSetVars().get(0);
        String password = new String(setPassVar.getPassword());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);

        sql = "SET PASSWORD = PASSWORD('testPass')";
        setStmt = (SetStmt) analyzeSuccess(sql);
        setPassVar = (SetPassVar) setStmt.getSetVars().get(0);
        password = new String(setPassVar.getPassword());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);

        sql = "SET PASSWORD = '*88EEBA7D913688E7278E2AD071FDB5E76D76D34B'";
        setStmt = (SetStmt) analyzeSuccess(sql);
        setPassVar = (SetPassVar) setStmt.getSetVars().get(0);
        password = new String(setPassVar.getPassword());
        Assert.assertEquals("*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", password);
    }
}
