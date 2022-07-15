package com.starrocks.sql.analyzer;

import com.starrocks.alter.AlterTest;
import org.junit.BeforeClass;
import org.junit.Test;


import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeBackupTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testShowBackup() {
        String sql = "SHOW BACKUP FROM test;";
        analyzeSuccess(sql);
        analyzeSuccess("SHOW BACKUP;");
        analyzeFail("SHOW BACKUP FROM test1;");
    }

}
