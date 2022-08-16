// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
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

    @Test
    public void TestAdminShowReplicaStatus() {
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM db1.tbl1;");
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)\n" +
                "WHERE STATUS = \"VERSION_ERROR\";");
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITIONs (p1, p2)\n" +
                "WHERE STATUS = \"VERSION_ERROR\";");
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM tbl1\n" +
                "WHERE STATUS != \"OK\";");

        analyzeFail("ADMIN SHOW REPLICA STATUS FROM tbl1 WHERE TabletId = '10001'",
                "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
        analyzeFail("ADMIN SHOW REPLICA STATUS FROM tbl1 WHERE STASUS = '10001'",
                "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
        analyzeFail("ADMIN SHOW REPLICA STATUS FROM tbl1 WHERE STASUS > 'OK'",
                "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
    }
}
