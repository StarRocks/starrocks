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
    public void testRestore() {
        analyzeSuccess(
                "restore snapshot example_db.`snapshot_1` from `example_repo` on ( `backup_tbl` ) properties ( \"backup_timestamp\"=\"2020-05-04-16-45-08\", \"replication_num\" = \"1\" )");
        RestoreStmt stmt = (RestoreStmt) analyzeSuccess(
                "restore snapshot example_db.`snapshot_1` from `example_repo` on ( `backup_tbl` ) properties ( \"backup_timestamp\"=\"2020-05-04-16-45-08\", \"replication_num\" = \"1\" )");
        Assert.assertEquals("default_cluster:example_db", stmt.getDbName());
    }
}
