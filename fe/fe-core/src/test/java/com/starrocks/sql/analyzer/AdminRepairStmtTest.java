// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * AdminRepairTable: ADMIN REPAIR TABLE table_name[ PARTITION (p1,...)];
 * AdminCancelRepairTable: ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];
 * AminCheckTablets: ADMIN CHECK TABLET (tablet_id1, tablet_id2, ...) PROPERTIES("type" = "...");
 */
public class AdminRepairStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAdminRepairTable() {
        AdminRepairTableStmt stmt = (AdminRepairTableStmt) analyzeSuccess("ADMIN REPAIR TABLE test;");
        Assert.assertTrue(stmt.isSupportNewPlanner());
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("test", stmt.getTblName());
        stmt = (AdminRepairTableStmt) analyzeSuccess("ADMIN REPAIR TABLE test PARTITION(p1, p2, p3);");
        Assert.assertEquals(Arrays.asList("p1", "p2", "p3"), stmt.getPartitions());
        Assert.assertEquals(4 * 3600L, stmt.getTimeoutS());
        analyzeSuccess("ADMIN REPAIR TABLE test PARTITIONs(p1, p2, p3)");
        // bad cases
        analyzeFail("ADMIN REPAIR TABLE");
        analyzeFail("ADMIN REPAIR TABLE test TEMPORARY PARTITION(p1, p2, p3);");
    }

    @Test
    public void testAdminCancelRepairTable() {
        AdminCancelRepairTableStmt stmt = (AdminCancelRepairTableStmt) analyzeSuccess("ADMIN cancel REPAIR TABLE test;");
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals("test", stmt.getTblName());
        stmt = (AdminCancelRepairTableStmt) analyzeSuccess("ADMIN CANCEL REPAIR TABLE test PARTITION(p1, p2, p3);");
        Assert.assertEquals(Arrays.asList("p1", "p2", "p3"), stmt.getPartitions());
        Assert.assertTrue(stmt.isSupportNewPlanner());
        analyzeFail("ADMIN CANCEL REPAIR TABLE");
        analyzeFail("ADMIN cancel REPAIR TABLE test TEMPORARY PARTITION(p1, p2, p3);");
    }

    @Test
    public void testAdminCheckTablets() {
        AdminCheckTabletsStmt stmt = (AdminCheckTabletsStmt) analyzeSuccess("ADMIN CHECK TABLET (10000, 10001) " +
                "PROPERTIES(\"type\" = \"consistency\");");
        Assert.assertFalse(stmt.getProperties().containsKey("type"));
        Assert.assertEquals("consistency", stmt.getType().name().toLowerCase());
        Assert.assertEquals(Long.valueOf(10001L), stmt.getTabletIds().get(1));
        Assert.assertEquals(RedirectStatus.FORWARD_NO_SYNC, stmt.getRedirectStatus());
        // bad cases
        analyzeFail("ADMIN CHECK TABLET (10000, 10001);");
        analyzeFail("ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"amory\" = \"consistency\";");
        analyzeFail("ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"amory\" = \"amory\",\"type\" = \"consistency\");");
        analyzeFail("ADMIN CHECK TABLET (10000, 10001) PROPERTIES(\"type\" = \"amory\");");
        analyzeFail("ADMIN CHECK PROPERTIES(\"type\" = \"consistency\");");
    }

}