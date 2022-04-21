// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzePredicateTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testArrayPredicate() {
        analyzeSuccess("select * from tarray where v3 is null");
        analyzeFail("select * from tarray where v3 between [1,2,3] and [4,5,6]",
                "HLL, BITMAP, PERCENTILE and ARRAY type couldn't as Predicate");
        analyzeFail("select * from tarray where v3 in ([1,2,3], [4,5,6])",
                "HLL, BITMAP, PERCENTILE and ARRAY type couldn't as Predicate");
    }
}
