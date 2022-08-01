// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
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
}
