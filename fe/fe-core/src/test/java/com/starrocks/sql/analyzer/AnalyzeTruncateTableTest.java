// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TruncateTableStmt;
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
       Assert.assertEquals("TRUNCATE TABLE `example_db`.`tbl`", stmt.toSql());
       stmt = (TruncateTableStmt) analyzeSuccess("TRUNCATE TABLE tbl PARTITION(p1, p2);");
       Assert.assertEquals("TRUNCATE TABLE `test`.`tbl`PARTITIONS (p1, p2)", stmt.toSql());
       Assert.assertEquals("tbl", stmt.getTblName());
       Assert.assertEquals("test", stmt.getDbName());
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
