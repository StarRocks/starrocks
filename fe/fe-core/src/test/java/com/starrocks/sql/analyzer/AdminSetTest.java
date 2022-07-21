// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AdminSetTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void TestAdminSetConfig() {
        analyzeSuccess("admin set frontend config(\"alter_table_timeout_second\" = \"60\");");
        analyzeFail("admin set frontend config;", "You have an error in your SQL syntax");
    }

    @Test
    public void TestAdminSetReplicaStatus() {
        AdminSetReplicaStatusStmt stmt = (AdminSetReplicaStatusStmt)analyzeSuccess(
                "admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"ok\");");
        Assert.assertEquals(10003, stmt.getTabletId());
        Assert.assertEquals(10001, stmt.getBackendId());
        Assert.assertEquals("OK", stmt.getStatus().name());

        analyzeFail("admin set replica status properties(\"backend_id\" = \"10001\",\"status\" = \"ok\");",
                "Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        analyzeFail("admin set replica status properties(\"tablet_id\" = \"10003\",\"status\" = \"ok\");",
                "Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        analyzeFail("admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\");",
                "Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        analyzeFail("admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"MISSING\");",
                "Do not support setting replica status as MISSING");
        analyzeFail("admin set replica status properties(\"unknown_config\" = \"10003\");",
                "Unknown property: unknown_config");
    }
}
