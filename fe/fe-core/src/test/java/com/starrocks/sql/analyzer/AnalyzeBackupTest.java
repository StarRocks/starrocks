package com.starrocks.sql.analyzer;

import com.starrocks.alter.AlterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeBackupTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testBackup() {
        String sql = "BACKUP SNAPSHOT test.snapshot_label2\n" +
                "TO example_repo\n" +
                "ON\n" +
                "(\n" +
                "t0,\n" +
                "t1\n" +
                ");";
        analyzeSuccess(sql);
    }

}
