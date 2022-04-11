// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzerDropTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testDropTables() {
        analyzeSuccess("drop table if exists test1 force");
        analyzeSuccess("drop table if exists test1");
        analyzeSuccess("drop table test1 force");
        analyzeSuccess("drop table db1.test1");
        analyzeFail("drop table exists test1");
    }

    @Test
    public void testDropView() {
        analyzeSuccess("drop view if exists view");
        analyzeSuccess("drop view test1");
        analyzeSuccess("drop view db1.test1");
        analyzeFail("drop view test1 force");
        analyzeFail("drop view exists test1");
    }


}