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

import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * AdminRepairTable: ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)];
 * AdminCancelRepairTable: ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];
 * AminCheckTablets: ADMIN CHECK TABLET (tablet_id1, tablet_id2, ...) PROPERTIES("type" = "...");
 */
public class AdminRepairStmtTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAdminRepairTable() {
        AdminRepairTableStmt stmt = (AdminRepairTableStmt) analyzeSuccess("ADMIN REPAIR TABLE test;");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("test", stmt.getTblName());
        stmt = (AdminRepairTableStmt) analyzeSuccess("ADMIN REPAIR TABLE test PARTITION(p1, p2, p3);");
        Assertions.assertEquals(Arrays.asList("p1", "p2", "p3"), stmt.getPartitions());
        Assertions.assertEquals(4 * 3600L, stmt.getTimeoutS());
        analyzeSuccess("ADMIN REPAIR TABLE test PARTITIONs(p1, p2, p3)");
        // bad cases
        analyzeFail("ADMIN REPAIR TABLE");
        analyzeFail("ADMIN REPAIR TABLE test TEMPORARY PARTITION(p1, p2, p3);");
    }

    @Test
    public void testAdminCancelRepairTable() {
        AdminCancelRepairTableStmt stmt = (AdminCancelRepairTableStmt) analyzeSuccess("ADMIN cancel REPAIR TABLE test;");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("test", stmt.getTblName());
        stmt = (AdminCancelRepairTableStmt) analyzeSuccess("ADMIN CANCEL REPAIR TABLE test PARTITION(p1, p2, p3);");
        Assertions.assertEquals(Arrays.asList("p1", "p2", "p3"), stmt.getPartitions());
        analyzeFail("ADMIN CANCEL REPAIR TABLE");
        analyzeFail("ADMIN cancel REPAIR TABLE test TEMPORARY PARTITION(p1, p2, p3);");
    }

    @Test
    public void testAdminCheckTablets() {
        AdminCheckTabletsStmt stmt = (AdminCheckTabletsStmt) analyzeSuccess("ADMIN CHECK TABLET (10000, 10001) " +
                "PROPERTIES(\"type\" = \"consistency\");");
        Assertions.assertTrue(stmt.getProperty().containsKey("type"));
        Assertions.assertEquals("consistency", stmt.getType().name().toLowerCase());
        Assertions.assertEquals(Long.valueOf(10001L), stmt.getTabletIds().get(1));
        // bad cases
        analyzeFail("ADMIN CHECK TABLET (10000, 10001);");
        analyzeFail("ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"amory\" = \"consistency\";");
        analyzeFail("ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"amory\" = \"amory\",\"type\" = \"consistency\");");
        analyzeFail("ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"type\" = \"amory\");");
        analyzeFail("ADMIN CHECK PROPERTIES(\"type\" = \"consistency\");");
    }

}