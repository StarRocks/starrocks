// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.RestoreStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeRestoreTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testBackup() {
        analyzeSuccess(
                "restore snapshot example_db1.`snapshot_1` from `example_repo` on ( `backup_tbl` ) properties ( \"backup_timestamp\"=\"2020-05-04-16-45-08\", \"replication_num\" = \"1\" )");
        RestoreStmt stmt = (RestoreStmt) analyzeSuccess(
                "restore snapshot example_db1.`snapshot_1` from `example_repo` on ( `backup_tbl` ) properties ( \"backup_timestamp\"=\"2020-05-04-16-45-08\", \"replication_num\" = \"1\" )");
        Assert.assertEquals("default_cluster:example_db1", stmt.getDbName());
        Assert.assertEquals(
                "RESTORE SNAPSHOT example_db1.`snapshot_1` FROM `example_repo` ON ( `backup_tbl` ) PROPERTIES ( \"backup_timestamp\"=\"2020-05-04-16-45-08\", \"replication_num\" = \"1\" )",
                stmt.toString());
    }
}
