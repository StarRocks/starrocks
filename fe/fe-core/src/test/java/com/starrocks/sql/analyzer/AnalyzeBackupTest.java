// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.BackupStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeBackupTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testBackup() {
        analyzeSuccess(
                "backup snapshot example_db.snapshot_label1 to example_repo on (example_tbl) PROPERTIES ( \"timeout\" = \"7200\" );");
        BackupStmt stmt = (BackupStmt) analyzeSuccess(
                "backup snapshot example_db.snapshot_label1 to example_repo on (example_tbl) PROPERTIES ( \"timeout\" = \"7200\" );");
        Assert.assertEquals("default_cluster:example_db", stmt.getDbName());
        Assert.assertEquals(
                "BACKUP SNAPSHOT example_db.snapshot_label1 TO example_repo ON (example_tbl) PROPERTIES ( \"timeout\" = \"7200\" );",
                stmt.toString());
    }
}