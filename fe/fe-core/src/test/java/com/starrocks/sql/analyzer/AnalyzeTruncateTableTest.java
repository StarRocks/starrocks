// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeTruncateTableTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void normalTest() {
        TruncateTableStmt stmt = (TruncateTableStmt) analyzeSuccess("TRUNCATE TABLE example_db.tbl;");
        Assert.assertEquals("tbl", stmt.getTblName());
        Assert.assertEquals("example_db", stmt.getDbName());

        stmt = (TruncateTableStmt) analyzeSuccess("TRUNCATE TABLE tbl PARTITION(p1, p2);");
        Assert.assertEquals("tbl", stmt.getTblName());
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals(stmt.getTblRef().getPartitionNames().getPartitionNames().toString(), "[p1, p2]");
    }

    @Test
    public void testSingle() {
        analyzeSuccess("truncate table t0");
        analyzeSuccess("truncate table t0 partition t0");
        analyzeSuccess("truncate table t0 partition \"t0\"");
    }

    @Test
    public void failureTest() {
        analyzeFail("TRUNCATE TABLE tbl PARTITION();");
    }
}
