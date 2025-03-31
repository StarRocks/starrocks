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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Test;

public class ScanPredicateExprReuseTest extends PlanTestBase {
    @Test
    public void test() throws Exception {
        // 1. all predicates can be pushed down to scan node, no need to reuse common expressions
        {
            String sql = "select * from t0 where v1 = 10 and v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 10, 2: v2 = 5");
        }
        {
            String sql = "select * from t0 where v1 = 10 or v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: (1: v1 = 10) OR (2: v2 = 5)");
        }
        {
            String sql = "select * from t0 where v1 in (10, 20) and v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 IN (10, 20), 2: v2 = 5");
        }
        {
            String sql = "select * from t0 where bit_shift_left(v1,1) = 10 and v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 BITSHIFTLEFT 1 = 10, 2: v2 = 5");
        }
        {
            String sql = "select * from t0 where v1 > v2";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 > 2: v2");
        }
        {
            String sql = "select * from t0 where v1 > v2 or v1 = 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, " PREDICATES: (1: v1 > 2: v2) OR (1: v1 = 10)");
        }
        // 2. all predicates can't be pushed down
        {
            String sql = "select * from tarray where all_match(x->x>10, v3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: all_match(array_map(<slot 4> -> <slot 4> > 10, 3: v3))");
        }
        // 3. some predicates can be pushed down
        {
            String sql = "select * from t0 where v1 + v2 > 10 and v1 + v2 + v3 > 20 and v1 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: 4: add > 10, 4: add + 3: v3 > 20\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 4> : 1: v1 + 2: v2\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 5");
        }
        {
            String sql = "select * from tarray where v1 = 10 and array_max(array_map(x->x + v2, v3)) > 1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: array_max(array_map(<slot 4> -> <slot 4> + 2: v2, 3: v3)) > 1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: tarray\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 10");
        }
        {
            String sql = "with input as (\n" +
                    "    select array_min(array_map(x->x+v1, v3)) as x from tarray\n" +
                    "),\n" +
                    "input2 as (\n" +
                    "    select x + 1 as a, x + 2 as b, x + 3 as c from input\n" +
                    ")\n" +
                    "select * from input2 where a + b + c < 10;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 6: expr + 7: expr + 8: expr < 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 6> : 10: array_min + 1\n" +
                    "  |  <slot 7> : 10: array_min + 2\n" +
                    "  |  <slot 8> : 10: array_min + 3\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 9> : array_map(<slot 4> -> <slot 4> + 1: v1, 3: v3)\n" +
                    "  |  <slot 10> : array_min(9: array_map)");
        }
    }

    @Test
    public void testLimit() throws Exception {
        {
            String sql = "select * from tarray where all_match(x->x>10, v3) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: all_match(array_map(<slot 4> -> <slot 4> > 10, 3: v3))\n" +
                    "  |  limit: 10");
        }
        {
            String sql = "select * from t0 where v1 + v2 > 10 and v1 + v2 + v3 > 20 and v1 = 5 limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: 4: add > 10, 4: add + 3: v3 > 20\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 4> : 1: v1 + 2: v2\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 5");
        }
    }

    @Test
    public void testComplexType() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `complex_t` (\n" +
                "  `k` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `v1` array<bigint(20)> NULL COMMENT \"\",\n" +
                "  `v2` array<bigint(20)> NULL COMMENT \"\",\n" +
                "  `v3` array<bigint(20)> NULL COMMENT \"\",\n" +
                "  `v4` struct<a int(11), b struct<a int(11)>> NULL COMMENT \"\",\n" +
                "  `v5` struct<a int(11), b struct<a array<bigint(20)>>> NULL COMMENT \"\",\n" +
                "  `v6` map<int(11),int(11)> NULL COMMENT \"\",\n" +
                "  `v7` map<int(11),int(11)> NULL COMMENT \"\",\n" +
                "  `v8` json NULL COMMENT \"\",\n" +
                "  `v9` json NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"compression\" = \"LZ4\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"fast_schema_evolution\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ")");
        // for complex type predicates, column pruning can still be used after pull up predicates from scan node
        {
            String sql = "select k from complex_t where v1[0] > v2[3]";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 11: element_at > 12: element_at\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  11 <-> 2: v1[0]\n" +
                    "  |  12 <-> 3: v2[3]");
            assertContains(plan, "     Pruned type: 2 <-> [ARRAY<BIGINT>]\n" +
                    "     Pruned type: 3 <-> [ARRAY<BIGINT>]\n" +
                    "     ColumnAccessPath: [/v1/INDEX, /v2/INDEX]");
        }
        {
            String sql = "select k from complex_t where array_length(v1) > array_length(v3)";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 11: array_length > 12: array_length\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  11 <-> array_length[([2: v1, ARRAY<BIGINT>, true]); args: INVALID_TYPE; " +
                    "result: INT; args nullable: true; result nullable: true]\n" +
                    "  |  12 <-> array_length[([4: v3, ARRAY<BIGINT>, true]); args: INVALID_TYPE; " +
                    "result: INT; args nullable: true; result nullable: true]\n" +
                    "  |  cardinality: 1");
            assertContains(plan, "     Pruned type: 2 <-> [ARRAY<BIGINT>]\n" +
                    "     Pruned type: 4 <-> [ARRAY<BIGINT>]\n" +
                    "     ColumnAccessPath: [/v1/OFFSET, /v3/OFFSET]");
        }
        {
            String sql = "select k from complex_t where array_length(array_map(x->x+10, v5.b.a)) > 10";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: array_length(array_map(<slot 11> -> <slot 11> + 10, 12: subfield)) > 10\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  12 <-> 6: v5.b.a[false]\n" +
                    "  |  cardinality: 1");
            assertContains(plan, "ColumnAccessPath: [/v5/b/a]");
        }
        {
            // json
            String sql = "select k from complex_t where v8->'$.a.b' > v9->'$.a.c'";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 11: json_query > 12: json_query\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  11 <-> json_query[([9: v8, JSON, true], '$.a.b'); args: JSON,VARCHAR; " +
                    "result: JSON; args nullable: true; result nullable: true]\n" +
                    "  |  12 <-> json_query[([10: v9, JSON, true], '$.a.c'); args: JSON,VARCHAR; " +
                    "result: JSON; args nullable: true; result nullable: true]\n" +
                    "  |  cardinality: 1");
            assertContains(plan, "ColumnAccessPath: [/v8/a/b(json), /v9/a/c(json)]");
        }
        {
            String sql = "select k from complex_t where json_length(v8, '$.k1') > json_length(v9->'$.k1.k2')";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 11: json_length > 12: json_length\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  11 <-> json_length[([9: v8, JSON, true], '$.k1'); args: JSON,VARCHAR; " +
                    "result: INT; args nullable: true; result nullable: true]\n" +
                    "  |  12 <-> json_length[(json_query[([10: v9, JSON, true], '$.k1.k2'); " +
                    "args: JSON,VARCHAR; result: JSON; args nullable: true; result nullable: true]); " +
                    "args: JSON; result: INT; args nullable: true; result nullable: true]");
            assertContains(plan, "ColumnAccessPath: [/v8/k1(json), /v9/k1/k2(json)]");
        }
        {
            // map
            String sql = "select k from complex_t where array_max(array_map((x,y)->x+y+k, map_keys(v6),map_keys(v7)))>10";
            String plan = getVerboseExplain(sql);
            assertContains("  2:SELECT\n" +
                    "  |  predicates: array_max(array_map((<slot 11>, <slot 12>) -> " +
                    "CAST(<slot 11> AS BIGINT) + CAST(<slot 12> AS BIGINT) + 1: k, 13: map_keys, 14: map_keys)) > 10\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  13 <-> map_keys[([7: v6, MAP<INT,INT>, true]); args: INVALID_TYPE; " +
                    "result: ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                    "  |  14 <-> map_keys[([8: v7, MAP<INT,INT>, true]); args: INVALID_TYPE; " +
                    "result: ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                    "  |  cardinality: 1");
            assertContains(plan, "     Pruned type: 7 <-> [MAP<INT,INT>]\n" +
                    "     Pruned type: 8 <-> [MAP<INT,INT>]\n" +
                    "     ColumnAccessPath: [/v6/KEY, /v7/KEY]");
        }
        {
            String sql = "select k from complex_t where v6[0] > v7[1]";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 11: element_at > 12: element_at\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  11 <-> 7: v6[0]\n" +
                    "  |  12 <-> 8: v7[1]");
            assertContains(plan, "     Pruned type: 7 <-> [MAP<INT,INT>]\n" +
                    "     Pruned type: 8 <-> [MAP<INT,INT>]\n" +
                    "     ColumnAccessPath: [/v6/INDEX, /v7/INDEX]");
        }
        {
            String sql = "select k from complex_t where cardinality(v1) + cardinality(v5.b.a) > 3 and " +
                    "cardinality(v1) + cardinality(v5.b.a) + cardinality(v6) >5";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 16: add > 3, 16: add + CAST(13: cardinality AS BIGINT) > 5\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 16> : 14: cast + 15: cast\n" +
                    "  |    <slot 14> : CAST(11: cardinality AS BIGINT)\n" +
                    "  |    <slot 15> : CAST(12: cardinality AS BIGINT)\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: k, BIGINT, false]\n" +
                    "  |  11 <-> cardinality[([2: v1, ARRAY<BIGINT>, true]); args: INVALID_TYPE; " +
                    "result: INT; args nullable: true; result nullable: true]\n" +
                    "  |  12 <-> cardinality[(6: v5.b.a[true]); args: INVALID_TYPE; result: INT; " +
                    "args nullable: true; result nullable: true]\n" +
                    "  |  13 <-> cardinality[([7: v6, MAP<INT,INT>, true]); args: INVALID_TYPE; " +
                    "result: INT; args nullable: true; result nullable: true]\n" +
                    "  |  cardinality: 1");
            assertContains(plan, "     Pruned type: 2 <-> [ARRAY<BIGINT>]\n" +
                    "     Pruned type: 6 <-> [struct<a int(11), b struct<a array<bigint(20)>>>]\n" +
                    "     Pruned type: 7 <-> [MAP<INT,INT>]\n" +
                    "     ColumnAccessPath: [/v1/OFFSET, /v5/b/a/OFFSET, /v6/OFFSET]");
        }

    }
}
