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

import org.junit.Test;

public class DeferProjectAfterTopNTest extends PlanTestBase {

    @Test
    public void testNormalCases() throws Exception {
        {
            String sql = "select v1, hex(v2), v2 + v3 from t0 order by v1 limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:TOP-N\n" +
                    "  |  order by: <slot 1> 1: v1 ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  3:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : hex(2: v2)\n" +
                    "  |  <slot 5> : 2: v2 + 3: v3\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  2:MERGING-EXCHANGE");
        }
        {
            String sql = "select v1,hex(v1) from t0 order by hex(v1) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:TOP-N\n" +
                    "  |  order by: <slot 4> 4: hex ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : hex(1: v1)");
        }
        {
            String sql = "select v1, hex(v2), hex(v3) from t0 order by v1,hex(v2) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:TOP-N\n" +
                    "  |  order by: <slot 1> 1: v1 ASC, <slot 4> 4: hex ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 3> : 3: v3\n" +
                    "  |  <slot 4> : hex(2: v2)\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  4:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: hex\n" +
                    "  |  <slot 5> : hex(3: v3)\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  3:MERGING-EXCHANGE");
        }
        {
            String sql = "select v1, hex(v2), length(hex(v2)) from t0 order by hex(v2) limit 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:TOP-N\n" +
                    "  |  order by: <slot 4> 4: hex ASC\n" +
                    "  |  offset: 0\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 4> : hex(2: v2)\n" +
                    "  |  \n" +
                    "  0:OlapScanNode");
            assertContains(plan, "  4:Project\n" +
                    "  |  <slot 1> : 1: v1\n" +
                    "  |  <slot 4> : 4: hex\n" +
                    "  |  <slot 5> : length(hex(2: v2))\n" +
                    "  |  limit: 10\n" +
                    "  |  \n" +
                    "  3:MERGING-EXCHANGE");
        }

    }

    @Test
    public void testComplexTypes() throws Exception {
        String sql = "select v1, array_length(v3) from tarray order by v1 limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  2:TOP-N\n" +
                "  |  order by: [1, BIGINT, true] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: v1), remote = false\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  4 <-> array_length[([3: v3, ARRAY<BIGINT>, true]); args: INVALID_TYPE; " +
                "result: INT; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: tarray, rollup: tarray\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=3.0\n" +
                "     Pruned type: 3 <-> [ARRAY<BIGINT>]\n" +
                "     ColumnAccessPath: [/v3/OFFSET]");

        sql = "select v1, v3, array_length(v3) from tarray order by v1 limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  3 <-> [3: v3, ARRAY<BIGINT>, true]\n" +
                "  |  4 <-> array_length[([3: v3, ARRAY<BIGINT>, true]); " +
                "args: INVALID_TYPE; result: INT; args nullable: true; result nullable: true]\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE");

        sql = "select v1, map_keys(v3) from tmap order by v1 limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:TOP-N\n" +
                "  |  order by: [1, BIGINT, true] ASC\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (<slot 1> 1: v1), remote = false\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  4 <-> map_keys[([3: v3, MAP<BIGINT,CHAR(20)>, true]); args: INVALID_TYPE; " +
                "result: ARRAY<BIGINT>; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: tmap, rollup: tmap\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=3.0\n" +
                "     Pruned type: 3 <-> [MAP<BIGINT,CHAR(20)>]\n" +
                "     ColumnAccessPath: [/v3/KEY]\n" +
                "     cardinality: 1");

        connectContext.getSessionVariable().setCboPruneJsonSubfield(true);
        sql = "select v_int, get_json_string(v_json, '$.k') from tjson order by v_int limit 10";
        plan = getVerboseExplain(sql);
        System.out.println(plan);
        assertContains(plan, "");

        sql = "select v_int, get_json_string(v_json, '$.k'), v_json from tjson order by v_int limit 10";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v_int, BIGINT, true]\n" +
                "  |  2 <-> [2: v_json, JSON, true]\n" +
                "  |  3 <-> get_json_string[([2: v_json, JSON, true], '$.k'); args: JSON,VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]\n" +
                "  |  limit: 10\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE");
        connectContext.getSessionVariable().setCboPruneJsonSubfield(false);
    }
}
