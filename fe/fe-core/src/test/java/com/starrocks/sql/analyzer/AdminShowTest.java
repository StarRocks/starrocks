// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AdminShowTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void TestAdminShowConfig() {
        analyzeSuccess("admin show frontend config;");
        analyzeSuccess("admin show frontend config like '%parallel%';");
    }

    @Test
    public void TestAdminShowReplicaDistribution() {
        analyzeSuccess("ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;");
        analyzeSuccess("ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);");
    }
}
