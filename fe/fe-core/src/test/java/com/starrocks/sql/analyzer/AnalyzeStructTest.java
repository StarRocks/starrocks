// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeStructTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testStructSubFieldAccess() {
        analyzeSuccess("select * from struct_a");
        analyzeSuccess("select b from struct_a");
        analyzeSuccess("select b.a from struct_a");

        // Qualified name test
        analyzeSuccess("select struct_a from struct_a");
        analyzeSuccess("select struct_a.struct_a from struct_a");
        analyzeSuccess("select test.struct_a.struct_a from struct_a");
        analyzeSuccess("select struct_a from struct_a");
        analyzeSuccess("select struct_a.other from struct_a");
        analyzeFail("select struct_a.p from struct_a");
        analyzeFail("select p.a from struct_a");
    }
}
