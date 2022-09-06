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
        analyzeSuccess("select * from struct_complex");
        analyzeSuccess("select test.struct_complex.b from struct_complex");
        analyzeSuccess("select b.a from struct_complex");
        analyzeSuccess("select d.c.a from struct_complex");
        analyzeFail("select d.c.x from struct_complex");
    }
}
