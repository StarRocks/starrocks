// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeShowTest {
    // may also start a Mocked Frontend
    private static final String runningDir = "fe/mocked/AnalyzeShow/" + UUID.randomUUID() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        AnalyzeTestUtil.init();
    }

    @Test
    public void testShowVariables() {
        analyzeSuccess("show variables");
        analyzeSuccess("show variables where variables_name = 't1'");
    }

    @Test
    public void testShowTableStatus() {
        analyzeSuccess("show table status;");
        analyzeSuccess("show table status where Name = 't1';");
    }

    @Test
    public void testShowDatabases() {
        analyzeSuccess("show databases;");
        analyzeSuccess("show databases where database = 't1';");
    }

    @Test
    public void testShowTables() {
        analyzeSuccess("show tables;");
        analyzeSuccess("show tables where table_name = 't1';");
    }

    @Test
    public void testShowColumns() {
        analyzeSuccess("show columns from t1;");
        analyzeSuccess("show columns from t1 where Field = 'v1';");
    }
}
