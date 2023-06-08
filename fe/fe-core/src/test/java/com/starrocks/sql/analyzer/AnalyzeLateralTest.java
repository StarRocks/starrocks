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

import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
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
                "Table Function clause cannot contain aggregations");
    }

    @Test
    public void testUpperCase() {
        analyzeSuccess("select v1,t.* from tarray, UNNEST(v3) t");
        analyzeSuccess("select * from tarray, UNNEST(v3, ['a', 'b', 'c']) as t(unnest_1, unnest_2)");
    }

    @Test
    public void testVarArgs() {
        analyzeSuccess("select * from tarray, unnest(v3) as t(unnest_1)");
        analyzeSuccess("select unnest_1 from tarray, unnest(v3) as t(unnest_1)");
        analyzeSuccess("select t.unnest_1 from tarray, unnest(v3) as t(unnest_1)");
        analyzeFail("select t.unnest_2 from tarray, unnest(v3) as t(unnest_1)");

        analyzeSuccess("select t.unnest_1 from tarray, unnest(v3, ['a', 'b', 'c']) as t(unnest_1, unnest_2)");
        StatementBase stmt =
                analyzeSuccess("select * from tarray, unnest(v3, ['a', 'b', 'c']) as t(unnest_1, unnest_2)");
        QueryRelation queryRelation = ((QueryStatement) stmt).getQueryRelation();
        Assert.assertEquals("[v1, v2, v3, v4, v5, unnest_1, unnest_2]", queryRelation.getColumnOutputNames().toString());

        analyzeFail("select * from tarray, unnest(v3, ['a', 'b', 'c']) as t(unnest_1)",
                "table t has 2 columns available but 1 columns specified");
        analyzeFail("select * from tarray, unnest(v3) as t(unnest_1, unnest_2)",
                "table t has 1 columns available but 2 columns specified");

        analyzeFail("select * from tarray, unnest(null, null) as t(unnest_1, unnest_2)", "Unknown table function");
        analyzeFail("select * from tarray, unnest(v3, null) as t(unnest_1, unnest_2)", "Unknown table function");
        analyzeFail("select * from tarray, unnest(null, v3) as t(unnest_1, unnest_2)", "Unknown table function");
    }
}