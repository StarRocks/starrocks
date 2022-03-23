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
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzePredicate/" + UUID.randomUUID() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        AnalyzeTestUtil.init();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
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
