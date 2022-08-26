// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeDeleteTest {
    private static String runningDir = "fe/mocked/AnalyzeDelete/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testPartitions() {
        DeleteStmt st;
        st = (DeleteStmt) SqlParser.parse("delete from tjson partition (p0)", 0).get(0);
        Assert.assertEquals(1, st.getPartitionNames().size());
        st = (DeleteStmt) SqlParser.parse("delete from tjson partition p0", 0).get(0);
        Assert.assertEquals(1, st.getPartitionNames().size());
        st = (DeleteStmt) SqlParser.parse("delete from tjson partition (p0, p1)", 0).get(0);
        Assert.assertEquals(2, st.getPartitionNames().size());
    }

    @Test
    public void testSingle() {
        StatementBase stmt = analyzeSuccess("delete from tjson where v_int = 1");
        Assert.assertEquals(false, ((DeleteStmt) stmt).supportNewPlanner());

        analyzeFail("delete from tjson",
                "Where clause is not set");

        stmt = analyzeSuccess("delete from tprimary where pk = 1");
        Assert.assertEquals(true, ((DeleteStmt) stmt).supportNewPlanner());

        analyzeFail("delete from tprimary partitions (p1, p2) where pk = 1",
                "Delete for primary key table do not support specifying partitions");

        analyzeFail("delete from tprimary",
                "Delete must specify where clause to prevent full table delete");
    }
}
