// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeLateralTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testUnnest() {
        analyzeSuccess("select * from tarray cross join unnest(v3)");
        analyzeSuccess("select * from tarray cross join lateral unnest(v3)");
        analyzeFail("select * from tarray left join unnest(v3) on true",
                "Not support lateral join except inner or cross");
        analyzeFail("select * from tarray left join lateral unnest(v3) on true",
                "Not support lateral join except inner or cross");
        analyzeSuccess("select * from tarray,unnest(v3)");
        analyzeSuccess("select * from tarray,lateral unnest(v3)");
        analyzeSuccess("select v1,unnest from tarray,unnest(v3)");
        analyzeSuccess("select v1,t.unnest from tarray, unnest(v3) t");
        analyzeSuccess("select v1,t.* from tarray, unnest(v3) t");

        analyzeFail("select * from tarray, unknow_table_function(v2)",
                "Unknown table function 'unknow_table_function(BIGINT)'");
        analyzeFail("select * from tarray,unnest(v2)", "Unknown table function 'unnest(BIGINT)'");
        analyzeFail("select * from tarray,unnest(foo)", "Column 'foo' cannot be resolved");
        analyzeFail("select * from t0 cross join lateral t1", "Only support lateral join with UDTF");
        analyzeFail("select  unnest(split('1,2,3',','))", "Table function cannot be used in expression");
        analyzeFail("select a.* from unnest(split('1,2,3',',')) a",
                "Table function must be used with lateral join");

        analyzeFail("select * from t0,unnest(bitmap_to_array(bitmap_union(to_bitmap(v1))))",
                "UNNEST clause cannot contain aggregations");
    }
}