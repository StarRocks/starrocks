package com.starrocks.sql.analyzer;

import com.starrocks.alter.AlterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzerRestoreTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testShowRestore() {
        String sql = "SHOW RESTORE FROM test;";
        analyzeSuccess(sql);
        analyzeSuccess("SHOW RESTORE;");
        analyzeFail("SHOW RESTORE FROM test1;");
    }
}
