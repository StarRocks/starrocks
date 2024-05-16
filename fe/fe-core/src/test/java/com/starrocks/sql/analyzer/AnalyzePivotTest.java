// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzePivotTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testPivot() {
        analyzeSuccess("select * from t0 pivot (sum(v1) for v2 in (1, 2, 3))");
        analyzeSuccess("select * from t0 pivot (sum(v1), count(*) for v2 in (1, 2, 3))");
        analyzeSuccess("select * from t0 pivot (sum(v1), count(*) for (v2, v3) in ((1,1), (2, 2), (3, 3)))");

        // alias
        analyzeSuccess("select * from t0 pivot (sum(v1) as s for v2 in (1, 2, 3))");
        analyzeSuccess("select * from t0 pivot (sum(v1) as s, count(*) as c for v2 in (1, 2, 3))");
        analyzeSuccess("select * from t0 " +
                "pivot (sum(v1) as s, count(*) as c for (v2, v3) in ((1,1), (2, 2), (3, 3)))");

        analyzeSuccess("select * from t0 pivot (sum(v1) as s for v2 in (1 as one, 2 as two, 3 as three))");
        analyzeSuccess("select * from t0 " +
                "pivot (sum(v1) as s, count(*) as c for v2 in (1 as one, 2 as two, 3 as three))");
        analyzeSuccess("select * from t0 " +
                "pivot (sum(v1) as s, count(*) as c for (v2, v3) in ((1,1) as one , (2, 2) as two, (3, 3) as three))");

        // null
        analyzeSuccess("select * from t0 pivot (sum(v1) as s for v2 in (1, 2, 3, null))");
        analyzeSuccess("select * from t0 " +
                "pivot (sum(v1) as s, count(*) as c for (v2, v3) in ((null, null) as one , (2, 2) as two, (3, 3) as three))");

        // order by
        analyzeSuccess("select * from t0 " +
                "pivot (count(*) as c for (v2, v3) in ((1,1) as one , (2, 2) as two, (3, 3) as three))" +
                "order by v1 limit 2");

        // type
        analyzeSuccess("select * from t0 pivot (sum(v1) for v2 in (1.5, 2.5, 3.5))");

        // not aggregate function
        analyzeFail("select * from t0 pivot (abs(v1) for v2 in (1, 2, 3))",
                "Measure expression in PIVOT must use aggregate function");
    }
}
