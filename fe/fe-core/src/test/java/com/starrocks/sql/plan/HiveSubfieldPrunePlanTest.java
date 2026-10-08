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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class HiveSubfieldPrunePlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @Test
    public void testMapReturningExprKeepsOtherColumnsPrunable() throws Exception {
        // The lambda inside map_filter, and a CASE WHEN whose branches return MAP, are expressions the
        // subfield visitor cannot reason about. They make col_map be read whole, which is expected, but
        // they must not stop col_struct from being pruned down to the single field the query reads.
        String sql = "select map_filter((k, v) -> k = 1, col_map), col_struct.c0 "
                + "from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_struct] <-> [struct<`c0` int(11)>]");

        sql = "select case when col_int = 1 then map_filter((k, v) -> k = 1, col_map) else col_map end, "
                + "col_struct.c0 from hive0.subfield_db.subfield_map";
        assertContains(getVerboseExplain(sql), "[col_struct] <-> [struct<`c0` int(11)>]");

        // map_values pushes MAP_VALUE before descending into the unknown expression. That path belongs
        // to map_filter's result, not to col_map, so col_map must keep both its keys and its values.
        sql = "select map_values(map_filter((k, v) -> k = 1, col_map)), col_struct.c0 "
                + "from hive0.subfield_db.subfield_map";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[col_struct] <-> [struct<`c0` int(11)>]");
        assertContains(plan, "[col_map] <-> [MAP<INT,INT>]");
    }

    @Test
    public void testUnknownExprDoesNotInheritDownstreamPath() throws Exception {
        // A lower operator defines m = map_filter(lambda, col_map) and an upper one reads map_values(m).
        // The MAP_VALUE path belongs to map_filter's result, not to col_map: attributing it would drop
        // col_map's keys, which the lambda evaluates. Here the join keeps the two in separate operators,
        // so the path arrives through the visited access group rather than through the local path stack.
        String sql = "select map_values(t.m) from "
                + "(select col_int, map_filter((k, v) -> k = 1, col_map) as m from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        assertContains(getVerboseExplain(sql), "[col_map] <-> [MAP<INT,INT>]");

        // Same shape across an aggregate, and with only map_keys downstream.
        sql = "with t as (select col_int, map_filter((k, v) -> k = 1, col_map) as m "
                + "from hive0.subfield_db.subfield_map) "
                + "select map_keys(t.m), s.col_int from t join (select col_int from hive0.subfield_db.subfield_map) s "
                + "on t.col_int = s.col_int";
        assertContains(getVerboseExplain(sql), "[col_map] <-> [MAP<INT,INT>]");
    }

    @Test
    public void testArrayReturningExprIsAPruneBoundary() throws Exception {
        // A lambda reads its argument's subfields through its own ColumnRefOperators, which never reach
        // the scan column, so those reads are invisible here. An outer subfield access must therefore not
        // narrow the column: select array_filter(x -> x.user = 'official', name)[1].family would read
        // name as struct<family> and the BE would fail evaluating x.user.
        String sql = "select array_map((x) -> named_struct('c0', x.c1), col_arr)[1].c0 "
                + "from hive0.subfield_db.subfield_map";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[col_arr] <-> [ARRAY<struct<`c0` int(11), `c1` int(11)>>]");

        // Same shape with the subfield access in a different operator, so the path arrives through the
        // visited access group instead of the local path stack.
        sql = "select t.a[1].c0 from "
                + "(select array_map((x) -> named_struct('c0', x.c1), col_arr) as a, col_int "
                + "from hive0.subfield_db.subfield_map) t "
                + "join (select col_int as ci from hive0.subfield_db.subfield_map) s on t.col_int = s.ci";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[col_arr] <-> [ARRAY<struct<`c0` int(11), `c1` int(11)>>]");
    }

    @Test
    public void testAggCTE() throws Exception {
        String sql = "with stream as (select array_agg(col_struct.c0) as t1 from hive0.subfield_db.subfield group by " +
                "col_int) select t1 from stream";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "Pruned type: 2 [col_struct] <-> [struct<`c0` int(11)>]");
    }
}
