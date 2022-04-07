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

public class AnalyzeArrayTest {
    private static String runningDir = "fe/mocked/AnalyzeAggregateTest/" + UUID.randomUUID().toString() + "/";

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
    public void testArray() {
        analyzeFail("select [1,2,3][1:2]", "Array slice is not currently supported");
        analyzeSuccess("select []");
        analyzeSuccess("select [][-1]");
        analyzeSuccess("select [1,2,3][v1] from t0");

        analyzeSuccess(" select\n" +
                "            array_contains([1], 1),\n" +
                "            array_contains(ARRAY<SMALLINT>[1], 1),\n" +
                "            array_contains(ARRAY<INT>[1], 1),\n" +
                "            array_contains(ARRAY<float>[1], cast(1.0 as float)),\n" +
                "            array_contains(ARRAY<double>[1], cast(1.0 as double)),\n" +
                "            array_contains(ARRAY<date>['2020-01-01', '2020-01-02'], cast('2020-01-2' as date)),\n" +
                "            array_contains(ARRAY<date>['2020-01-01', '2020-01-02'], cast('2020-01-1' as date)),\n" +
                "            array_contains(['x', 'y', null], null),\n" +
                "            array_contains(['x', 'y', null], 'x'),\n" +
                "            array_contains(['x', 'y', null], 'y'),\n" +
                "            array_contains([true, false, true], true),\n" +
                "            array_contains([true, false, true], false)");
    }
}
