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

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ArraySortLambdaTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.unitTestView = false;
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `s1` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int(11) NULL COMMENT \"\",    \n" +
                "  `s1` struct<count bigint, sum double, avg double> NULL COMMENT \"\",    \n" +
                "  `a1` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `a2` array<varchar(65533)> NULL COMMENT \"\",   \n" +
                "  `a3` array<bigint> NULL COMMENT \"\",    \n" +
                "  `a4` array<double> NULL COMMENT \"\",    \n" +
                "  `a5` array<int> NULL COMMENT \"\",    \n" +
                "  `a6` array<smallint> NULL COMMENT \"\",    \n" +
                "  `a7` array<array<smallint>> NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "DUPLICATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",       \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"true\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"light_schema_change\" = \"true\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");
    }

    @Test
    public void testCountDistinctWithPartition() throws Exception {
        String sqlFmt = "select array_sort(%s) from s1";
        String[][] testCases = new String[][] {
                {"a2, (x,y)->case x < y when true then -1 when false then 1 else NULL end",
                        "(<slot 11>, <slot 12>) -> " +
                                "CASE <slot 11> < <slot 12> WHEN TRUE THEN -1 WHEN FALSE THEN 1 ELSE NULL END < 0"},
                {"a2, (x,y)->x < y", "(<slot 11>, <slot 12>) -> <slot 11> < <slot 12>"},
                {"a3, (x,y)->x - y", "(<slot 11>, <slot 12>) -> <slot 11> - <slot 12> < 0"},
                {"a4, (x,y)->x - y", "(<slot 11>, <slot 12>) -> <slot 11> - <slot 12> < 0.0"},
                {"a5, (x,y)->x - y", "(<slot 11>, <slot 12>) -> " +
                        "CAST(<slot 11> AS BIGINT) - CAST(<slot 12> AS BIGINT) < 0"},
                {"a6, (x,y)->x - y", "(<slot 11>, <slot 12>) -> CAST(<slot 11> AS INT) - CAST(<slot 12> AS INT) < 0"},
                {"a6, (x,y)->x < y", "(<slot 11>, <slot 12>) -> <slot 11> < <slot 12>"},
                {"a7, (x,y)->array_length(x) < array_length(y)", "(<slot 11>, <slot 12>) -> " +
                        "array_length(<slot 11>) < array_length(<slot 12>)"}
        };
        for (String[] tc : testCases) {
            String s = tc[0];
            String expect = tc[1];
            String sql = String.format(sqlFmt, s);
            String plan = getFragmentPlan(sql);
            assertCContains(plan, expect);
        }
    }

    @Test
    public void testIllegalLambdaFunction() {
        String sqlFmt = "select array_sort([3,4,1,5,7],(x,y)-> %s) from s1;";
        String[] exprs = new String[] {
                "rand()",
                "x",
                "0.5",
                "x-y*s1",
                "s1",
                "s1 < s2",
        };
        for (String e : exprs) {
            String sql = String.format(sqlFmt, e);
            Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        }
    }

    @Test
    public void testLambdaReturnDouble() throws Exception {
        String sql = "select array_sort(cast([0.4,0.2,0.1,0.2] as array<double>), (x,y)->x-y)";
        String plan = getVerboseExplain(sql);
        assertCContains(plan, "([2, DOUBLE, true], [3, DOUBLE, true]) -> [2, DOUBLE, true] - [3, DOUBLE, true] < 0.0)");
    }
}