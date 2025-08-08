// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AdminShowTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAdminShowConfig() {
        analyzeSuccess("admin show frontend config;");
        analyzeSuccess("admin show frontend config like '%parallel%';");
    }

    @Test
    public void testAdminShowReplicaDistribution() {
        analyzeSuccess("ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;");
        analyzeSuccess("ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);");
    }

    @Test
    public void testAdminShowReplicaStatus() {
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM db1.tbl1;");
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2)\n" +
                "WHERE STATUS = \"VERSION_ERROR\";");
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITIONs (p1, p2)\n" +
                "WHERE STATUS = \"VERSION_ERROR\";");
        analyzeSuccess("ADMIN SHOW REPLICA STATUS FROM tbl1\n" +
                "WHERE STATUS != \"OK\";");

        analyzeFail("ADMIN SHOW REPLICA STATUS FROM tbl1 WHERE TabletId = '10001'",
                "Where clause can only be 'status =|!= 'OK'|'DEAD'|'VERSION_ERROR'|'SCHEMA_ERROR'|'MISSING''");
        analyzeFail("ADMIN SHOW REPLICA STATUS FROM tbl1 WHERE STASUS = '10001'",
                "Where clause can only be 'status =|!= 'OK'|'DEAD'|'VERSION_ERROR'|'SCHEMA_ERROR'|'MISSING''");
        analyzeFail("ADMIN SHOW REPLICA STATUS FROM tbl1 WHERE STASUS > 'OK'",
                "Where clause can only be 'status =|!= 'OK'|'DEAD'|'VERSION_ERROR'|'SCHEMA_ERROR'|'MISSING''");
    }

    @Test
    public void testDescribe() {
        DescribeStmt stmt = (DescribeStmt) analyzeSuccess("desc test.t0");
        Assertions.assertEquals(6, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("test", stmt.getDb());
        Assertions.assertEquals("t0", stmt.getTableName());

        stmt = (DescribeStmt) analyzeSuccess("desc test.t0 all");
        Assertions.assertEquals(8, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("test", stmt.getDb());
        Assertions.assertEquals("t0", stmt.getTableName());
        Assertions.assertTrue(stmt.isAllTables());
    }

    @Test
    public void testDescExplain() {
        String sql = "desc insert into test.t0 select * from test.t0";
        InsertStmt insertStmt = (InsertStmt) analyzeSuccess(sql);
        Assertions.assertTrue(insertStmt.isExplain());
    }
}
